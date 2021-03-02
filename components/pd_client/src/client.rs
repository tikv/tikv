// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::channel::mpsc;
use futures::compat::Future01CompatExt;
use futures::executor::block_on;
use futures::future::{self, BoxFuture, FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};

use grpcio::{CallOption, EnvBuilder, Environment, Result as GrpcResult, WriteFlags};
use kvproto::metapb;
use kvproto::pdpb::{self, Member};
use kvproto::replication_modepb::{RegionReplicationStatus, ReplicationStatus};
use security::SecurityManager;
use tikv_util::time::duration_to_sec;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::{box_err, debug, error, info, thd_name, warn};
use tikv_util::{Either, HandyRwLock};
use txn_types::TimeStamp;
use yatp::task::future::TaskCell;
use yatp::ThreadPool;

use super::metrics::*;
use super::util::{check_resp_header, sync_request, validate_endpoints, LeaderClient};
use super::{Config, FeatureGate, PdFuture, UnixSecs};
use super::{Error, PdClient, RegionInfo, RegionStat, Result, REQUEST_TIMEOUT};

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "pd";

pub struct RpcClient {
    cluster_id: u64,
    leader_client: Arc<LeaderClient>,
    monitor: Arc<ThreadPool<TaskCell>>,
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
            v => v.checked_add(1).unwrap_or(std::isize::MAX),
        };
        let monitor = Arc::new(
            yatp::Builder::new(thd_name!("pdmonitor"))
                .max_thread_count(1)
                .build_future_pool(),
        );
        for i in 0..retries {
            match validate_endpoints(Arc::clone(&env), cfg, security_mgr.clone()).await {
                Ok((client, members)) => {
                    let rpc_client = RpcClient {
                        cluster_id: members.get_header().get_cluster_id(),
                        leader_client: Arc::new(LeaderClient::new(
                            env,
                            security_mgr,
                            client,
                            members,
                        )),
                        monitor: monitor.clone(),
                    };

                    // spawn a background future to update PD information periodically
                    let duration = cfg.update_interval.0;
                    let client = Arc::downgrade(&rpc_client.leader_client);
                    let update_loop = async move {
                        loop {
                            let ok = GLOBAL_TIMER_HANDLE
                                .delay(Instant::now() + duration)
                                .compat()
                                .await
                                .is_ok();

                            if !ok {
                                warn!("failed to delay with global timer");
                                continue;
                            }

                            match client.upgrade() {
                                Some(cli) => {
                                    let req = cli.reconnect().await;
                                    if req.is_err() {
                                        warn!("update PD information failed");
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

                    return Ok(rpc_client);
                }
                Err(e) => {
                    if i as usize % cfg.retry_log_every == 0 {
                        warn!("validate PD endpoints failed"; "err" => ?e);
                    }
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(Instant::now() + cfg.retry_interval.0)
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
        self.leader_client.get_leader()
    }

    /// Re-establishes connection with PD leader in synchronized fashion.
    pub fn reconnect(&self) -> Result<()> {
        block_on(self.leader_client.reconnect())
    }

    /// Creates a new call option with default request timeout.
    #[inline]
    fn call_option() -> CallOption {
        CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT))
    }

    /// Gets given key's Region and Region's leader from PD.
    fn get_region_and_leader(
        &self,
        key: &[u8],
    ) -> PdFuture<(metapb::Region, Option<metapb::Peer>)> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_region"])
            .start_coarse_timer();

        let mut req = pdpb::GetRegionRequest::default();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let executor = move |client: &LeaderClient, req: pdpb::GetRegionRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_region_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_async_opt", e)
                });

            Box::pin(async move {
                let mut resp = handler.await?;
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
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_store_and_stats(&self, store_id: u64) -> PdFuture<(metapb::Store, pdpb::StoreStats)> {
        let timer = Instant::now();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let executor = move |client: &LeaderClient, req: pdpb::GetStoreRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_store_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "get_store_async", e));

            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_store_async"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                let store = resp.take_store();
                if store.get_state() != metapb::StoreState::Tombstone {
                    Ok((store, resp.take_stats()))
                } else {
                    Err(Error::StoreTombstone(format!("{:?}", store)))
                }
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }
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

impl PdClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(
        &self,
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

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.bootstrap_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;
        Ok(resp.replication_status.take())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["is_cluster_bootstrapped"])
            .start_coarse_timer();

        let mut req = pdpb::IsBootstrappedRequest::default();
        req.set_header(self.header());

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.is_bootstrapped_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["alloc_id"])
            .start_coarse_timer();

        let mut req = pdpb::AllocIdRequest::default();
        req.set_header(self.header());

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.alloc_id_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["put_store"])
            .start_coarse_timer();

        let mut req = pdpb::PutStoreRequest::default();
        req.set_header(self.header());
        req.set_store(store);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.put_store_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.replication_status.take())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_store"])
            .start_coarse_timer();

        let mut req = pdpb::GetStoreRequest::default();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_store_opt(&req, Self::call_option())
        })?;
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
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_all_stores"])
            .start_coarse_timer();

        let mut req = pdpb::GetAllStoresRequest::default();
        req.set_header(self.header());
        req.set_exclude_tombstone_stores(exclude_tombstone);

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_all_stores_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp.take_stores().into())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_cluster_config"])
            .start_coarse_timer();

        let mut req = pdpb::GetClusterConfigRequest::default();
        req.set_header(self.header());

        let mut resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_cluster_config_opt(&req, Self::call_option())
        })?;
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

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let executor = move |client: &LeaderClient, req: pdpb::GetRegionByIdRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_region_by_id_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_by_id", e)
                });
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_region_by_id"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_region() {
                    Ok(Some(resp.take_region()))
                } else {
                    Ok(None)
                }
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_region_leader_by_id(
        &self,
        region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        let timer = Instant::now();

        let mut req = pdpb::GetRegionByIdRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let executor = move |client: &LeaderClient, req: pdpb::GetRegionByIdRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_region_by_id_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_by_id", e)
                });
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_region_by_id"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_region() {
                    Ok(Some((resp.take_region(), resp.take_leader())))
                } else {
                    Ok(None)
                }
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
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
        req.set_approximate_size(region_stat.approximate_size);
        req.set_approximate_keys(region_stat.approximate_keys);
        if let Some(s) = replication_status {
            req.set_replication_status(s);
        }
        let mut interval = pdpb::TimeInterval::default();
        interval.set_start_timestamp(region_stat.last_report_ts.into_inner());
        interval.set_end_timestamp(UnixSecs::now().into_inner());
        req.set_interval(interval);

        let executor = |client: &LeaderClient, req: pdpb::RegionHeartbeatRequest| {
            let mut inner = client.inner.wl();
            if let Either::Left(ref mut left) = inner.hb_sender {
                debug!("heartbeat sender is refreshed");
                let sender = left.take().expect("expect region heartbeat sink");
                let (tx, rx) = mpsc::unbounded();
                inner.hb_sender = Either::Right(tx);
                inner.client_stub.spawn(async move {
                    let mut sender = sender.sink_map_err(Error::Grpc);
                    let result = sender
                        .send_all(&mut rx.map(|r| Ok((r, WriteFlags::default()))))
                        .await;
                    match result {
                        Ok(()) => {
                            sender.get_mut().cancel();
                            info!("cancel region heartbeat sender");
                        }
                        Err(e) => {
                            error!(?e; "failed to send heartbeat");
                        }
                    };
                });
            }

            let sender = inner
                .hb_sender
                .as_mut()
                .right()
                .expect("expect region heartbeat sender");
            let ret = sender
                .unbounded_send(req)
                .map_err(|e| Error::Other(Box::new(e)));
            Box::pin(future::ready(ret)) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn handle_region_heartbeat_response<F>(&self, _: u64, f: F) -> PdFuture<()>
    where
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        self.leader_client.handle_region_heartbeat_response(f)
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let timer = Instant::now();

        let mut req = pdpb::AskSplitRequest::default();
        req.set_header(self.header());
        req.set_region(region);

        let executor = move |client: &LeaderClient, req: pdpb::AskSplitRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .ask_split_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "ask_split", e));

            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as PdFuture<_>
        };

        self.leader_client
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

        let executor = move |client: &LeaderClient, req: pdpb::AskBatchSplitRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .ask_batch_split_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "ask_batch_split", e));

            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_batch_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp)
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn store_heartbeat(
        &self,
        mut stats: pdpb::StoreStats,
    ) -> PdFuture<pdpb::StoreHeartbeatResponse> {
        let timer = Instant::now();

        let mut req = pdpb::StoreHeartbeatRequest::default();
        req.set_header(self.header());
        stats
            .mut_interval()
            .set_end_timestamp(UnixSecs::now().into_inner());
        req.set_stats(stats);
        let executor = move |client: &LeaderClient, req: pdpb::StoreHeartbeatRequest| {
            let feature_gate = client.feature_gate.clone();
            let handler = client
                .inner
                .rl()
                .client_stub
                .store_heartbeat_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "store_heartbeat", e));
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["store_heartbeat"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                match feature_gate.set_version(resp.get_cluster_version()) {
                    Err(_) => warn!("invalid cluster version: {}", resp.get_cluster_version()),
                    Ok(true) => info!("set cluster version to {}", resp.get_cluster_version()),
                    _ => {}
                };
                Ok(resp)
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_batch_split(&self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_regions(regions.into());

        let executor = move |client: &LeaderClient, req: pdpb::ReportBatchSplitRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .report_batch_split_async_opt(&req, Self::call_option())
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "report_batch_split", e)
                });
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["report_batch_split"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn scatter_region(&self, mut region: RegionInfo) -> Result<()> {
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

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.scatter_region_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())
    }

    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, f: F) {
        self.leader_client.on_reconnect(Box::new(f))
    }

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        let timer = Instant::now();

        let mut req = pdpb::GetGcSafePointRequest::default();
        req.set_header(self.header());

        let executor = move |client: &LeaderClient, req: pdpb::GetGcSafePointRequest| {
            let option = CallOption::default().timeout(Duration::from_secs(REQUEST_TIMEOUT));
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_gc_safe_point_async_opt(&req, option)
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_gc_saft_point", e)
                });
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_gc_safe_point"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(resp.get_safe_point())
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn get_store_stats_async(&self, store_id: u64) -> BoxFuture<'_, Result<pdpb::StoreStats>> {
        self.get_store_and_stats(store_id).map_ok(|x| x.1).boxed()
    }

    fn get_operator(&self, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_operator"])
            .start_coarse_timer();

        let mut req = pdpb::GetOperatorRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let resp = sync_request(&self.leader_client, LEADER_CHANGE_RETRY, |client| {
            client.get_operator_opt(&req, Self::call_option())
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp)
    }
    // TODO: The current implementation is not efficient, because it creates
    //       a RPC for every `PdFuture<TimeStamp>`. As a duplex streaming RPC,
    //       we could use one RPC for many `PdFuture<TimeStamp>`.
    fn get_tso(&self) -> PdFuture<TimeStamp> {
        let timer = Instant::now();

        let mut req = pdpb::TsoRequest::default();
        req.set_count(1);
        req.set_header(self.header());
        let executor = move |client: &LeaderClient, req: pdpb::TsoRequest| {
            let cli = client.inner.rl();
            let (mut req_sink, mut resp_stream) = cli
                .client_stub
                .tso()
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "tso", e));
            let send_once = async move {
                req_sink.send((req, WriteFlags::default())).await?;
                req_sink.close().await?;
                GrpcResult::Ok(())
            }
            .map(|_| ());
            cli.client_stub.spawn(send_once);
            Box::pin(async move {
                let resp = resp_stream.try_next().await?;
                let resp = match resp {
                    Some(r) => r,
                    None => return Ok(TimeStamp::zero()),
                };
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["tso"])
                    .observe(duration_to_sec(timer.elapsed()));
                check_resp_header(resp.get_header())?;
                let ts = resp.get_timestamp();
                let encoded = TimeStamp::compose(ts.physical as _, ts.logical as _);
                Ok(encoded)
            }) as PdFuture<_>
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn feature_gate(&self) -> &FeatureGate {
        &self.leader_client.feature_gate
    }
}

pub struct DummyPdClient {
    pub next_ts: TimeStamp,
}

impl DummyPdClient {
    pub fn new() -> DummyPdClient {
        DummyPdClient {
            next_ts: TimeStamp::zero(),
        }
    }
}

impl PdClient for DummyPdClient {
    fn get_tso(&self) -> PdFuture<TimeStamp> {
        Box::pin(future::ok(self.next_ts))
    }
}
