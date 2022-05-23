// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
    u64,
};

use futures::{
    channel::mpsc,
    compat::{Compat, Future01CompatExt},
    executor::block_on,
    future::{self, BoxFuture, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use grpcio::{CallOption, EnvBuilder, Environment, WriteFlags};
use kvproto::{
    metapb,
    pdpb::{self, Member},
    replication_modepb::{RegionReplicationStatus, ReplicationStatus, StoreDrAutoSyncStatus},
};
use security::SecurityManager;
use tikv_util::{
    box_err, debug, error, info, thd_name,
    time::{duration_to_sec, Instant},
    timer::GLOBAL_TIMER_HANDLE,
    warn, Either, HandyRwLock,
};
use txn_types::TimeStamp;
use yatp::{task::future::TaskCell, ThreadPool};

use super::{
    metrics::*,
    util::{check_resp_header, sync_request, Client, PdConnector},
    BucketStat, Config, Error, FeatureGate, PdClient, PdFuture, RegionInfo, RegionStat, Result,
    UnixSecs, REQUEST_TIMEOUT,
};

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "pd";

pub struct RpcClient {
    cluster_id: u64,
    pd_client: Arc<Client>,
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
            v => v.saturating_add(1),
        };
        let monitor = Arc::new(
            yatp::Builder::new(thd_name!("pdmonitor"))
                .max_thread_count(1)
                .build_future_pool(),
        );
        let pd_connector = PdConnector::new(env.clone(), security_mgr.clone());
        for i in 0..retries {
            match pd_connector.validate_endpoints(cfg).await {
                Ok((client, target, members, tso)) => {
                    let cluster_id = members.get_header().get_cluster_id();
                    let rpc_client = RpcClient {
                        cluster_id,
                        pd_client: Arc::new(Client::new(
                            Arc::clone(&env),
                            security_mgr.clone(),
                            client,
                            members,
                            target,
                            tso,
                            cfg.enable_forwarding,
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

    /// Creates a new call option with default request timeout.
    #[inline]
    pub fn call_option(client: &Client) -> CallOption {
        client
            .inner
            .rl()
            .target_info()
            .call_option()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT))
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

        let executor = move |client: &Client, req: pdpb::GetRegionRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_region_async_opt(&req, Self::call_option(client))
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
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_store_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "get_store_async", e));

            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_store_async"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
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
    fn load_global_config(&self, list: Vec<String>) -> PdFuture<HashMap<String, String>> {
        use kvproto::pdpb::LoadGlobalConfigRequest;
        let mut req = LoadGlobalConfigRequest::new();
        req.set_names(list.into());
        let executor = |client: &Client, req| match client
            .inner
            .rl()
            .client_stub
            .clone()
            .load_global_config_async(&req)
        {
            Ok(grpc_response) => Box::pin(async move {
                match grpc_response.await {
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
            }) as PdFuture<_>,
            Err(err) => Box::pin(async move { Err(box_err!("{:?}", err)) }) as PdFuture<_>,
        };
        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn watch_global_config(
        &self,
    ) -> Result<grpcio::ClientSStreamReceiver<pdpb::WatchGlobalConfigResponse>> {
        use kvproto::pdpb::WatchGlobalConfigRequest;
        let req = WatchGlobalConfigRequest::default();
        sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.watch_global_config(&req)
        })
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
            .with_label_values(&["bootstrap_cluster"])
            .start_coarse_timer();

        let mut req = pdpb::BootstrapRequest::default();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.bootstrap_opt(&req, Self::call_option(&self.pd_client))
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

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.is_bootstrapped_opt(&req, Self::call_option(&self.pd_client))
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

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.alloc_id_opt(&req, Self::call_option(&self.pd_client))
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

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.put_store_opt(&req, Self::call_option(&self.pd_client))
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

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.get_store_opt(&req, Self::call_option(&self.pd_client))
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

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.get_all_stores_opt(&req, Self::call_option(&self.pd_client))
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

        let mut resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.get_cluster_config_opt(&req, Self::call_option(&self.pd_client))
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

        let executor = move |client: &Client, req: pdpb::GetRegionByIdRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_region_by_id_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_by_id", e)
                });
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_region_by_id"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_region() {
                    Ok(Some(resp.take_region()))
                } else {
                    Ok(None)
                }
            }) as PdFuture<_>
        };

        self.pd_client
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

        let executor = move |client: &Client, req: pdpb::GetRegionByIdRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .get_region_by_id_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "get_region_by_id", e)
                });
            Box::pin(async move {
                let mut resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["get_region_by_id"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                if resp.has_region() && resp.has_leader() {
                    Ok(Some((resp.take_region(), resp.take_leader())))
                } else {
                    Ok(None)
                }
            }) as PdFuture<_>
        };

        self.pd_client
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
            if let Either::Left(ref mut left) = inner.hb_sender {
                debug!("heartbeat sender is refreshed");
                let sender = left.take().expect("expect region heartbeat sink");
                let (tx, rx) = mpsc::unbounded();
                let pending_heartbeat = Arc::new(AtomicU64::new(0));
                inner.hb_sender = Either::Right(tx);
                inner.pending_heartbeat = pending_heartbeat.clone();
                inner.client_stub.spawn(async move {
                    let mut sender = sender.sink_map_err(Error::Grpc);
                    let mut last_report = u64::MAX;
                    let result = sender
                        .send_all(&mut rx.map(|r| {
                            let last = pending_heartbeat.fetch_sub(1, Ordering::Relaxed);
                            // Sender will update pending at every send operation, so as long as
                            // pending task is increasing, pending count should be reported by
                            // sender.
                            if last + 10 < last_report || last == 1 {
                                PD_PENDING_HEARTBEAT_GAUGE.set(last as i64 - 1);
                                last_report = last;
                            }
                            if last > last_report {
                                last_report = last - 1;
                            }
                            Ok((r, WriteFlags::default()))
                        }))
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

            let last = inner.pending_heartbeat.fetch_add(1, Ordering::Relaxed);
            PD_PENDING_HEARTBEAT_GAUGE.set(last as i64 + 1);
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
            let handler = client
                .inner
                .rl()
                .client_stub
                .ask_split_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "ask_split", e));

            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_split"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
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
            let handler = client
                .inner
                .rl()
                .client_stub
                .ask_batch_split_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "ask_batch_split", e));

            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["ask_batch_split"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
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
            let handler = client
                .inner
                .rl()
                .client_stub
                .store_heartbeat_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "store_heartbeat", e));
            Box::pin(async move {
                let resp = handler.await?;
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
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_batch_split(&self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        let timer = Instant::now();

        let mut req = pdpb::ReportBatchSplitRequest::default();
        req.set_header(self.header());
        req.set_regions(regions.into());

        let executor = move |client: &Client, req: pdpb::ReportBatchSplitRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .report_batch_split_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| {
                    panic!("fail to request PD {} err {:?}", "report_batch_split", e)
                });
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["report_batch_split"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as PdFuture<_>
        };

        self.pd_client
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

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.scatter_region_opt(&req, Self::call_option(&self.pd_client))
        })?;
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
            let option = Self::call_option(client);
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
                    .observe(duration_to_sec(timer.saturating_elapsed()));
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
        let _timer = PD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["get_operator"])
            .start_coarse_timer();

        let mut req = pdpb::GetOperatorRequest::default();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let resp = sync_request(&self.pd_client, LEADER_CHANGE_RETRY, |client| {
            client.get_operator_opt(&req, Self::call_option(&self.pd_client))
        })?;
        check_resp_header(resp.get_header())?;

        Ok(resp)
    }

    fn batch_get_tso(&self, count: u32) -> PdFuture<TimeStamp> {
        let begin = Instant::now();
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
                    .with_label_values(&["tso"])
                    .observe(duration_to_sec(begin.saturating_elapsed()));
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
        let begin = Instant::now();
        let mut req = pdpb::UpdateServiceGcSafePointRequest::default();
        req.set_header(self.header());
        req.set_service_id(name.into());
        req.set_ttl(ttl.as_secs() as _);
        req.set_safe_point(safe_point.into_inner());
        let executor = move |client: &Client, r: pdpb::UpdateServiceGcSafePointRequest| {
            let handler = client
                .inner
                .rl()
                .client_stub
                .update_service_gc_safe_point_async_opt(&r, Self::call_option(client))
                .unwrap_or_else(|e| {
                    panic!(
                        "fail to request PD {} err {:?}",
                        "update_service_safe_point", e
                    )
                });
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["update_service_safe_point"])
                    .observe(duration_to_sec(begin.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
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
            let handler = client
                .inner
                .rl()
                .client_stub
                .report_min_resolved_ts_async_opt(&req, Self::call_option(client))
                .unwrap_or_else(|e| panic!("fail to request PD {} err {:?}", "min_resolved_ts", e));
            Box::pin(async move {
                let resp = handler.await?;
                PD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["min_resolved_ts"])
                    .observe(duration_to_sec(timer.saturating_elapsed()));
                check_resp_header(resp.get_header())?;
                Ok(())
            }) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
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
            if let Either::Left(ref mut left) = inner.buckets_sender {
                debug!("region buckets sender is refreshed");
                let sender = left.take().expect("expect report region buckets sink");
                let (tx, rx) = mpsc::unbounded();
                let pending_buckets = Arc::new(AtomicU64::new(0));
                inner.buckets_sender = Either::Right(tx);
                inner.pending_buckets = pending_buckets.clone();
                let resp = inner.buckets_resp.take().unwrap();
                inner.client_stub.spawn(async {
                    let res = resp.await;
                    warn!("region buckets stream exited: {:?}", res);
                });
                inner.client_stub.spawn(async move {
                    let mut sender = sender.sink_map_err(Error::Grpc);
                    let mut last_report = u64::MAX;
                    let result = sender
                        .send_all(&mut rx.map(|r| {
                            let last = pending_buckets.fetch_sub(1, Ordering::Relaxed);
                            // Sender will update pending at every send operation, so as long as
                            // pending task is increasing, pending count should be reported by
                            // sender.
                            if last + 10 < last_report || last == 1 {
                                PD_PENDING_BUCKETS_GAUGE.set(last as i64 - 1);
                                last_report = last;
                            }
                            if last > last_report {
                                last_report = last - 1;
                            }
                            Ok((r, WriteFlags::default()))
                        }))
                        .await;
                    match result {
                        Ok(()) => {
                            sender.get_mut().cancel();
                            info!("cancel region buckets sender");
                        }
                        Err(e) => {
                            error!(?e; "failed to send region buckets");
                        }
                    };
                });
            }

            let last = inner.pending_buckets.fetch_add(1, Ordering::Relaxed);
            PD_PENDING_BUCKETS_GAUGE.set(last as i64 + 1);
            let sender = inner
                .buckets_sender
                .as_mut()
                .right()
                .expect("expect region buckets sender");
            let ret = sender
                .unbounded_send(req)
                .map_err(|e| Error::Other(Box::new(e)));
            Box::pin(future::ready(ret)) as PdFuture<_>
        };

        self.pd_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
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

impl Default for DummyPdClient {
    fn default() -> Self {
        Self::new()
    }
}

impl PdClient for DummyPdClient {
    fn batch_get_tso(&self, _count: u32) -> PdFuture<TimeStamp> {
        Box::pin(future::ok(self.next_ts))
    }
}
