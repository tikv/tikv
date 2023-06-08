// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    ffi::CString,
    iter::FromIterator,
    marker::PhantomData,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
    time::Duration,
};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use fail::fail_point;
use futures::{compat::Future01CompatExt, future::select_all, FutureExt, TryFutureExt};
use grpcio::{
    ChannelBuilder, CompressionAlgorithms, Environment, Error as GrpcError, RpcStatusCode,
};
use kvproto::{
    kvrpcpb::{
        ApplySafeTsRequest, ApplySafeTsResponse, CheckLeaderRequest, CheckLeaderResponse,
        CheckedLeader, LeaderInfo,
    },
    metapb::{Peer, PeerRole},
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use protobuf::Message;
use raftstore::{
    router::CdcHandle,
    store::{msg::Callback, util::RegionReadProgressRegistry},
};
use security::SecurityManager;
use tikv_util::{
    info,
    sys::thread::ThreadBuildWrapper,
    time::{Instant, SlowTimer},
    timer::SteadyTimer,
    worker::Scheduler,
    future::paired_future_callback,
    Either,
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{Mutex, Notify},
};
use txn_types::TimeStamp;

use crate::{
    endpoint::Task,
    metrics::*,
};

const DEFAULT_CHECK_LEADER_TIMEOUT_DURATION: Duration = Duration::from_secs(5); // 5s
const DEFAULT_GRPC_GZIP_COMPRESSION_LEVEL: usize = 2;
const DEFAULT_GRPC_MIN_MESSAGE_SIZE_TO_COMPRESS: usize = 4096;

pub struct AdvanceTsWorker<T: 'static + CdcHandle<E>, E: KvEngine> {
    pd_client: Arc<dyn PdClient>,
    advance_ts_interval: Duration,
    timer: SteadyTimer,
    worker: Runtime,
    scheduler: Scheduler<Task>,
    /// The concurrency manager for transactions. It's needed for CDC to check
    /// locks when calculating resolved_ts.
    concurrency_manager: ConcurrencyManager,
    cdc_handler: Option<T>,
    _phantom: PhantomData<E>,
}

impl<T: 'static + CdcHandle<E>, E: KvEngine> AdvanceTsWorker<T, E> {
    pub fn new(
        advance_ts_interval: Duration,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        concurrency_manager: ConcurrencyManager,
        cdc_handler: Option<T>,
    ) -> Self {
        let worker = Builder::new_multi_thread()
            .thread_name("advance-ts")
            .worker_threads(1)
            .enable_time()
            .after_start_wrapper(|| {})
            .before_stop_wrapper(|| {})
            .build()
            .unwrap();
        Self {
            scheduler,
            pd_client,
            worker,
            advance_ts_interval,
            timer: SteadyTimer::default(),
            concurrency_manager,
            cdc_handler,
            _phantom: Default::default(),
        }
    }
}

impl<T: 'static + CdcHandle<E>, E: KvEngine> AdvanceTsWorker<T, E> {
    // Advance ts asynchronously and register RegisterAdvanceEvent when its done.
    pub fn advance_ts_for_regions(
        &self,
        mut leader_resolver: LeadershipResolver,
        advance_ts_interval: Duration,
        advance_notify: Arc<Notify>,
    ) {
        let cm = self.concurrency_manager.clone();
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let timeout = self.timer.delay(advance_ts_interval);
        let min_timeout = self.timer.delay(cmp::min(
            DEFAULT_CHECK_LEADER_TIMEOUT_DURATION,
            self.advance_ts_interval,
        ));
        let cdc_handler = self.cdc_handler.clone();
        let start = Instant::now();

        let fut = async move {
            // Ignore get tso errors since we will retry every `advdance_ts_interval`.
            let mut min_ts = pd_client.get_tso().await.unwrap_or_default();
            let get_pd_millis = start.saturating_elapsed().as_millis();
            
            let (cb, resp) = paired_future_callback();
            if let Err(e) = scheduler.schedule(Task::GetRegions { cb }) {
                info!("failed to schedule get_regions event"; "err" => ?e);
            }
            let regions = resp.await.unwrap_or_default();
            let get_regions_millis = start.saturating_elapsed().as_millis() - get_pd_millis;

            // Sync with concurrency manager so that it can work correctly when
            // optimizations like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTs` task, and the
            // resolver must be checked in or after `Task::MinTs`' execution.
            cm.update_max_ts(min_ts);
            if let Some(min_mem_lock_ts) = cm.global_min_lock_ts() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                }
            }

            let regions = leader_resolver
                .resolve(Either::Right(regions), min_ts, cdc_handler)
                .await;
            let elapsed = start.saturating_elapsed().as_millis();
            let resolve_millis = elapsed - get_pd_millis - get_regions_millis;
            info!("DBG resolve regions done";
                "elapsed" => elapsed,
                "resolve_millis" => resolve_millis,
                "get_regions_millis" => get_regions_millis,
                "get_pd_millis" => get_pd_millis);
            // let regions = if false {
            // } else {
            //     // Possible optimization here.
            //     // 1. Use `resolve_by_raft` to get regions with valid leader states.
            //     // 2. Send `CheckLeaderRequest` requests with as less information as
            // possible     //  for valid leader regions, for example raft
            // information like `term`,     // `epoch` could be saved. The proto
            // needs to be changed accordingly.     resolve_by_raft(regions,
            // min_ts, cdc_handler).await };

            if !regions.is_empty() {
                if let Err(e) = scheduler.schedule(Task::ResolvedTsAdvanced {
                    regions,
                    ts: min_ts,
                }) {
                    info!("failed to schedule advance event"; "err" => ?e);
                }
            }

            futures::select! {
                _ = timeout.compat().fuse() => (),
                // Skip wait timeout if a notify is arrived.
                _ = advance_notify.notified().fuse() => (),
            };
            // Wait min timeout to prevent from overloading advancing resolved ts.
            let _ = min_timeout.compat().await;

            // NB: We must schedule the leader resolver even if there is no region,
            //     otherwise we can not advance resolved ts next time.
            if let Err(e) = scheduler.schedule(Task::AdvanceResolvedTs { leader_resolver }) {
                error!("failed to schedule register advance event"; "err" => ?e);
            }
        };
        self.worker.spawn(fut);
    }
}

pub struct LeadershipResolver {
    tikv_clients: Mutex<HashMap<u64, TikvClient>>,
    pd_client: Arc<dyn PdClient>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    region_read_progress: RegionReadProgressRegistry,
    store_id: u64,

    buffer: ResolverBuffer,
}

struct ResolverBuffer {
    last_gc_time: Instant,
    gc_interval: Duration,
    // store_id -> check leader request, record the request to each stores.
    store_req_map: HashMap<u64, CheckLeaderRequest>,
    // region_id -> region, cache the information of regions.
    region_map: HashMap<u64, Vec<Peer>>,
    // region_id -> peers id, record the responses.
    resp_map: HashMap<u64, Vec<u64>>,
    valid_regions: HashSet<u64>,
    // store_id -> checking region_id sent to this store.
    store_region_map: HashMap<u64, Vec<u64>>,
    failed_region_store_map: HashMap<u64, Vec<u64>>,
    store_unsafe_regions_map: HashMap<u64, Vec<u64>>,
    valid_leader_infos: Vec<(LeaderInfo, Vec<u64>)>,
    store_checked_leader_map: HashMap<u64, Vec<CheckedLeader>>,
}

impl ResolverBuffer {
    fn new(gc_interval: Duration) -> Self {
        ResolverBuffer {
            gc_interval,
            last_gc_time: Instant::now_coarse(),
            store_req_map: HashMap::default(),
            region_map: HashMap::default(),
            resp_map: HashMap::default(),
            valid_regions: HashSet::default(),
            store_region_map: HashMap::default(),
            failed_region_store_map: HashMap::default(),
            store_unsafe_regions_map: HashMap::default(),
            valid_leader_infos: Vec::default(),
            store_checked_leader_map: HashMap::default(),
        }
    }

    fn clear(&mut self) {
        for v in self.store_req_map.values_mut() {
            v.regions.clear();
            v.inactive_regions.clear();
            v.ts = 0;
        }
        for v in self.region_map.values_mut() {
            v.clear();
        }
        for v in self.resp_map.values_mut() {
            v.clear();
        }
        for v in self.store_region_map.values_mut() {
            v.clear();
        }
        self.valid_regions.clear();
        for v in self.failed_region_store_map.values_mut() {
            v.clear();
        }
        for v in self.store_unsafe_regions_map.values_mut() {
            v.clear();
        }
        self.valid_leader_infos.clear();
        for v in self.store_checked_leader_map.values_mut() {
            v.clear();
        }
    }

    // release the memory of buffer and return whether the GC take effects.
    fn gc(&mut self) -> bool {
        let now = Instant::now_coarse();
        if now - self.last_gc_time > self.gc_interval {
            self.store_req_map = HashMap::default();
            self.region_map = HashMap::default();
            self.resp_map = HashMap::default();
            self.valid_regions = HashSet::default();
            self.store_region_map = HashMap::default();
            self.failed_region_store_map = HashMap::default();
            self.store_unsafe_regions_map = HashMap::default();
            self.valid_leader_infos = Vec::default();
            self.store_checked_leader_map = HashMap::default();
            self.last_gc_time = now;
            return true;
        }
        false
    }
}

impl LeadershipResolver {
    pub fn new(
        store_id: u64,
        pd_client: Arc<dyn PdClient>,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        region_read_progress: RegionReadProgressRegistry,
        gc_interval: Duration,
    ) -> LeadershipResolver {
        LeadershipResolver {
            tikv_clients: Mutex::default(),
            store_id,
            pd_client,
            env,
            security_mgr,
            region_read_progress,
            buffer: ResolverBuffer::new(gc_interval),
        }
    }

    // Confirms leadership of region peer before trying to advance resolved ts.
    // This function broadcasts a special message to all stores, gets the leader id
    // of them to confirm whether current peer has a quorum which accepts its
    // leadership.
    pub async fn resolve<T: 'static + CdcHandle<E>, E: KvEngine>(
        &mut self,
        regions: Either<Vec<u64>, HashMap<u64, Option<u64>>>,
        min_ts: TimeStamp,
        cdc_handler: Option<T>,
    ) -> Vec<u64> {
        let resolve_start = Instant::now();
        let regions = match regions {
            Either::Left(vec_regions) => {
                if vec_regions.is_empty() {
                    return vec_regions;
                }
                vec_regions
                    .into_iter()
                    .map(|region_id| (region_id, None))
                    .collect()
            }
            Either::Right(map_regions) => {
                if map_regions.is_empty() {
                    return Vec::new();
                }
                map_regions
            }
        };

        PENDING_RTS_COUNT.inc();
        defer!(PENDING_RTS_COUNT.dec());
        fail_point!("before_sync_replica_read_state", |_| regions.clone());

        let store_id = self.store_id;

        let buffer = &mut self.buffer;
        // GC when necessary to prevent memory leak.
        if !buffer.gc() {
            // Clear previous result before reusing buffer and resolving.
            buffer.clear();
        }
        let valid_regions = &mut buffer.valid_regions;
        let region_map = &mut buffer.region_map;
        let resp_map = &mut buffer.resp_map;
        let store_req_map = &mut buffer.store_req_map;
        let store_region_map = &mut buffer.store_region_map;

        let mut active_regions = 0;
        let mut read_index_reqs = Vec::new();

        // init regions need to be check.
        self.region_read_progress.with(|registry| {
            for (region_id, read_progress) in registry {
                let applied_index = match regions.get(region_id) {
                    Some(index) => index,
                    None => continue,
                };
                let mut core = read_progress.get_core();
                let is_inactive = applied_index
                    .map_or(false, |ref applied_index| !core.is_inactive(applied_index));
                if !is_inactive && applied_index.is_some() {
                    core.set_last_applied_index(applied_index.unwrap());
                }
                let local_leader_info = core.get_local_leader_info();
                let leader_id = local_leader_info.get_leader_id();
                let leader_store_id = local_leader_info.get_leader_store_id();
                let peer_list = local_leader_info.get_peers();
                // Check if the leader in this store.
                if leader_store_id != Some(store_id) {
                    continue;
                }
                let leader_info = core.get_leader_info();
                // check leader for action regions by read-index request.
                match (cdc_handler.clone(), is_inactive) {
                    (Some(cdc_handler), false) => {
                        active_regions += 1;
                        let cdc_handler = cdc_handler.clone();
                        let leader_info = leader_info.clone();
                        let region_id = *region_id;
                        let peer_store_ids = peer_list.iter().map(|peer| peer.store_id).collect();
                        let req = check_leader_by_raft(
                            cdc_handler,
                            leader_info,
                            region_id,
                            peer_store_ids,
                        )
                        .boxed();
                        read_index_reqs.push(req);
                        continue;
                    }
                    _ => {}
                }
                let mut unvotes = 0;
                for peer in peer_list {
                    if peer.store_id == store_id && peer.id == leader_id {
                        resp_map
                            .entry(*region_id)
                            .or_insert_with(|| Vec::with_capacity(peer_list.len()))
                            .push(store_id);
                    } else {
                        // It's still necessary to check leader on learners even if they don't vote
                        // because performing stale read on learners require it.
                        let req = store_req_map.entry(peer.store_id).or_insert_with(|| {
                            let mut req = CheckLeaderRequest::default();
                            req.inactive_regions = Vec::with_capacity(registry.len()).into();
                            req.set_store_id(store_id);
                            req
                        });
                        if is_inactive {
                            req.inactive_regions.push(leader_info.region_id);
                        } else {
                            req.regions.push(leader_info.clone());
                        }
                        if peer.get_role() != PeerRole::Learner {
                            unvotes += 1;
                        }
                        store_region_map
                            .entry(peer.store_id)
                            .or_insert_with(|| Vec::with_capacity(registry.len()))
                            .push(*region_id);
                    }
                }
                // Check `region_has_quorum` here because `store_map` can be empty,
                // in which case `region_has_quorum` won't be called any more.
                if unvotes == 0 && region_has_quorum(peer_list, &resp_map[region_id]) {
                    valid_regions.insert(*region_id);
                } else {
                    region_map
                        .entry(*region_id)
                        .or_insert_with(|| Vec::with_capacity(peer_list.len()))
                        .extend_from_slice(peer_list);
                }
            }
        });

        // create check-leader rpc futures.
        let env = &self.env;
        let pd_client = &self.pd_client;
        let security_mgr = &self.security_mgr;
        let tikv_clients = &self.tikv_clients;
        // Approximate `LeaderInfo` size
        let leader_info_size = store_req_map
            .values()
            .find(|req| !req.regions.is_empty())
            .map_or(0, |req| req.regions[0].compute_size()) as usize;
        let store_count = store_req_map.len();
        let mut check_leader_rpcs = Vec::with_capacity(store_req_map.len());
        for (store_id, req) in store_req_map {
            // TODO: send empty request to push resolve ts of hibernated regions.
            if req.regions.is_empty() && req.inactive_regions.is_empty() {
                continue;
            }
            let to_store = *store_id;
            let env = env.clone();
            let rpc = async move {
                let client = get_tikv_client(to_store, pd_client, security_mgr, env, tikv_clients)
                    .await
                    .map_err(|e| (to_store, e.retryable(), format!("[get tikv client] {}", e)))?;
                check_leader_by_rpc(to_store, client, req, min_ts, leader_info_size).await
            }
            .boxed();
            check_leader_rpcs.push(rpc);
        }
        check_leader_rpcs.extend(read_index_reqs);
        let start = Instant::now_coarse();

        defer!({
            RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC
                .with_label_values(&["all"])
                .observe(start.saturating_elapsed_secs());
        });
        info!("DBG p1 before send"; "check_leader_count" => check_leader_rpcs.len(), "regions" => regions.len(), "elapsed" => resolve_start.saturating_elapsed().as_millis());
        // handle check-leader results.
        let check_leader_count = check_leader_rpcs.len();
        let failed_region_store_map = &mut buffer.failed_region_store_map;
        let valid_leader_infos = &mut buffer.valid_leader_infos;
        for _ in 0..check_leader_count {
            // Use `select_all` to avoid the process getting blocked when some
            // TiKVs were down.
            let (res, _, remains) = select_all(check_leader_rpcs).await;
            check_leader_rpcs = remains;
            match res {
                Ok(CheckLeaderResult::ReadIndex(leader_info)) => {
                    if let Some(leader_info) = leader_info {
                        valid_leader_infos.push(leader_info);
                    }
                }
                Ok(CheckLeaderResult::CheckLeader(to_store, mut resp)) => {
                    let failed_regions = HashSet::from_iter(resp.take_failed_regions());
                    for region in &failed_regions {
                        failed_region_store_map
                            .entry(*region)
                            .or_insert_with(|| Vec::with_capacity(3))
                            .push(to_store);
                    }
                    let regions = store_region_map.get(&to_store).unwrap();
                    for region in regions {
                        if failed_regions.contains(&region) {
                            continue;
                        }
                        resp_map
                            .entry(*region)
                            .or_insert_with(|| Vec::with_capacity(store_count))
                            .push(to_store);
                    }
                }
                Err((to_store, reconnect, _err)) => {
                    if reconnect {
                        self.tikv_clients.lock().await.remove(&to_store);
                    }
                }
            }
        }
        info!("DBG p2 after send"; "elapsed" => resolve_start.saturating_elapsed().as_millis());

        let store_unsafe_regions_map = &mut buffer.store_unsafe_regions_map;
        for (region_id, prs) in region_map {
            if prs.is_empty() {
                // The peer had the leadership before, but now it's no longer
                // the case. Skip checking the region.
                continue;
            }
            if let Some(resp) = resp_map.get(region_id) {
                if resp.is_empty() {
                    // No response, maybe the peer lost leadership.
                    continue;
                }
                if region_has_quorum(prs, resp) {
                    valid_regions.insert(*region_id);
                } else {
                    // This is an invalid leader, should send apply_safe_ts request with
                    // unsafe_regions.
                    failed_region_store_map.get(&region_id).map(|stores| {
                        for store in stores {
                            store_unsafe_regions_map
                                .entry(*store)
                                .or_insert_with(|| {
                                    Vec::with_capacity(failed_region_store_map.len())
                                })
                                .push(*region_id);
                        }
                    });
                }
            }
        }

        let store_checked_leader_map = &mut buffer.store_checked_leader_map;
        for leader_info in valid_leader_infos.into_iter() {
            for peer_store_id in &leader_info.1 {
                let mut checked_leader = CheckedLeader::default();
                checked_leader.set_region_id(leader_info.0.get_region_id());
                checked_leader.set_read_state(leader_info.0.get_read_state().clone());

                store_checked_leader_map
                    .entry(*peer_store_id)
                    .or_insert_with(|| Vec::new())
                    .push(checked_leader);
            }
        }

        let mut apply_safe_ts_rpcs = Vec::with_capacity(store_unsafe_regions_map.len());
        for (to_store, unsafe_regions) in store_unsafe_regions_map {
            let env = env.clone();
            let mut req = ApplySafeTsRequest::default();
            req.set_ts(min_ts.into_inner());
            req.set_unsafe_regions(unsafe_regions.to_owned());
            req.set_store_id(store_id);
            if let Some(checked_leaders) = store_checked_leader_map.remove(&to_store) {
                req.set_checked_leaders(checked_leaders.into());
            }
            let to_store = *to_store;

            // Apply safe_ts for regions besides `unsafe_reginos` on `to_store`.
            let rpc = async move {
                let client = get_tikv_client(to_store, pd_client, security_mgr, env, tikv_clients)
                    .await
                    .map_err(|e| (to_store, e.retryable(), format!("[get tikv client] {}", e)))?;

                // let slow_timer = SlowTimer::default();
                // defer!({
                //     slow_log!(
                //         T
                //         slow_timer,
                //         "check leader rpc costs too long, to_store: {}",
                //         to_store
                //     );
                //     let elapsed = slow_timer.saturating_elapsed();
                //     RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC
                //         .with_label_values(&["rpc"])
                //         .observe(elapsed.as_secs_f64());
                // });

                let rpc = match client.apply_safe_ts_async(&req) {
                    Ok(rpc) => rpc,
                    Err(GrpcError::RpcFailure(status))
                        if status.code() == RpcStatusCode::UNIMPLEMENTED =>
                    {
                        // Some stores like TiFlash don't implement it.
                        return Ok((to_store, ApplySafeTsResponse::default()));
                    }
                    Err(e) => return Err((to_store, true, format!("[rpc create failed]{}", e))),
                };

                PENDING_CHECK_LEADER_REQ_SENT_COUNT.inc();
                defer!(PENDING_CHECK_LEADER_REQ_SENT_COUNT.dec());
                let timeout = DEFAULT_CHECK_LEADER_TIMEOUT_DURATION;
                let resp = tokio::time::timeout(timeout, rpc)
                    .map_err(|e| (to_store, true, format!("[timeout] {}", e)))
                    .await?
                    .map_err(|e| (to_store, true, format!("[rpc failed] {}", e)))?;
                Ok((to_store, resp))
            }
            .boxed();
            apply_safe_ts_rpcs.push(rpc);
        }

        info!("DBG p3 before send apply"; "apply_safe_ts_rpcs" => apply_safe_ts_rpcs.len(), "elapsed" => resolve_start.saturating_elapsed().as_millis());

        let resps = futures::future::join_all(apply_safe_ts_rpcs).await;
        for resp in resps.into_iter() {
            match resp {
                Ok(_) => {}
                Err((to_store, reconnect, err)) => {
                    info!("apply ts failed failed"; "error" => ?err, "to_store" => to_store);
                    if reconnect {
                        self.tikv_clients.lock().await.remove(&to_store);
                    }
                }
            }
        }
        info!("DBG p4 after send apply"; "elapsed" => resolve_start.saturating_elapsed().as_millis());

        valid_regions.drain().collect()
    }
}

// CheckLeaderResult union read-index result and check-leader result.
enum CheckLeaderResult {
    ReadIndex(Option<(LeaderInfo, Vec<u64>)>),
    CheckLeader(u64, CheckLeaderResponse),
}

async fn check_leader_by_rpc(
    to_store: u64,
    client: TikvClient,
    req: &mut CheckLeaderRequest,
    min_ts: TimeStamp,
    leader_info_size: usize,
) -> std::result::Result<CheckLeaderResult, (u64, bool, std::string::String)> {
    let region_num = req.regions.len();
    CHECK_LEADER_REQ_SIZE_HISTOGRAM.observe((leader_info_size * region_num) as f64);
    PENDING_CHECK_LEADER_REQ_COUNT.inc();
    defer!(PENDING_CHECK_LEADER_REQ_COUNT.dec());

    info!("DBG check_leader_by_rpc"; "inactives" => req.get_inactive_regions().len(), "min_ts" => min_ts);

    // Set min_ts in the request.
    req.set_ts(min_ts.into_inner());
    let slow_timer = SlowTimer::default();
    defer!({
        slow_log!(
            T
            slow_timer,
            "check leader rpc costs too long, to_store: {}",
            to_store
        );
        let elapsed = slow_timer.saturating_elapsed();
        RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC
            .with_label_values(&["rpc"])
            .observe(elapsed.as_secs_f64());
    });

    let rpc = match client.check_leader_async(req) {
        Ok(rpc) => rpc,
        Err(GrpcError::RpcFailure(status)) if status.code() == RpcStatusCode::UNIMPLEMENTED => {
            // Some stores like TiFlash don't implement it.
            return Ok(CheckLeaderResult::CheckLeader(
                to_store,
                CheckLeaderResponse::default(),
            ));
        }
        Err(e) => return Err((to_store, true, format!("[rpc create failed]{}", e))),
    };

    PENDING_CHECK_LEADER_REQ_SENT_COUNT.inc();
    defer!(PENDING_CHECK_LEADER_REQ_SENT_COUNT.dec());
    let timeout = DEFAULT_CHECK_LEADER_TIMEOUT_DURATION;
    let resp = tokio::time::timeout(timeout, rpc)
        .map_err(|e| (to_store, true, format!("[timeout] {}", e)))
        .await?
        .map_err(|e| (to_store, true, format!("[rpc failed] {}", e)))?;
    Ok(CheckLeaderResult::CheckLeader(to_store, resp))
}

async fn check_leader_by_raft<T: 'static + CdcHandle<E>, E: KvEngine>(
    cdc_handler: T,
    leader_info: LeaderInfo,
    region_id: u64,
    peer_store_ids: Vec<u64>,
) -> std::result::Result<CheckLeaderResult, (u64, bool, std::string::String)> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let callback = Callback::read(Box::new(move |resp| {
        let resp = if resp.response.get_header().has_error() {
            None
        } else {
            Some(region_id)
        };
        if tx.send(resp).is_err() {
            error!("resolve send tso response failed"; "region_id" => region_id);
        }
    }));
    if let Err(e) = cdc_handler.check_leadership(region_id, callback) {
        warn!("resolve send LeaderCallback failed"; "err" => ?e);
        return Ok(CheckLeaderResult::ReadIndex(None));
    }
    match rx.await {
        Ok(_) => Ok(CheckLeaderResult::ReadIndex(Some((
            leader_info,
            peer_store_ids,
        )))),
        Err(_) => Ok(CheckLeaderResult::ReadIndex(None)),
    }
}

pub async fn resolve_by_raft<T, E>(regions: Vec<u64>, min_ts: TimeStamp, cdc_handle: T) -> Vec<u64>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
{
    let mut reqs = Vec::with_capacity(regions.len());
    for region_id in regions {
        let cdc_handle_clone = cdc_handle.clone();
        let req = async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let callback = Callback::read(Box::new(move |resp| {
                let resp = if resp.response.get_header().has_error() {
                    None
                } else {
                    Some(region_id)
                };
                if tx.send(resp).is_err() {
                    error!("cdc send tso response failed"; "region_id" => region_id);
                }
            }));
            if let Err(e) = cdc_handle_clone.check_leadership(region_id, callback) {
                warn!("cdc send LeaderCallback failed"; "err" => ?e, "min_ts" => min_ts);
                return None;
            }
            rx.await.unwrap_or(None)
        };
        reqs.push(req);
    }

    let resps = futures::future::join_all(reqs).await;
    resps.into_iter().flatten().collect::<Vec<u64>>()
}

fn region_has_quorum(peers: &[Peer], stores: &[u64]) -> bool {
    let mut voters = 0;
    let mut incoming_voters = 0;
    let mut demoting_voters = 0;

    let mut resp_voters = 0;
    let mut resp_incoming_voters = 0;
    let mut resp_demoting_voters = 0;

    peers.iter().for_each(|peer| {
        let mut in_resp = false;
        for store_id in stores {
            if *store_id == peer.store_id {
                in_resp = true;
                break;
            }
        }
        match peer.get_role() {
            PeerRole::Voter => {
                voters += 1;
                if in_resp {
                    resp_voters += 1;
                }
            }
            PeerRole::IncomingVoter => {
                incoming_voters += 1;
                if in_resp {
                    resp_incoming_voters += 1;
                }
            }
            PeerRole::DemotingVoter => {
                demoting_voters += 1;
                if in_resp {
                    resp_demoting_voters += 1;
                }
            }
            PeerRole::Learner => (),
        }
    });

    let has_incoming_majority =
        (resp_voters + resp_incoming_voters) >= ((voters + incoming_voters) / 2 + 1);
    let has_demoting_majority =
        (resp_voters + resp_demoting_voters) >= ((voters + demoting_voters) / 2 + 1);

    has_incoming_majority && has_demoting_majority
}

static CONN_ID: AtomicI32 = AtomicI32::new(0);

async fn get_tikv_client(
    store_id: u64,
    pd_client: &Arc<dyn PdClient>,
    security_mgr: &SecurityManager,
    env: Arc<Environment>,
    tikv_clients: &Mutex<HashMap<u64, TikvClient>>,
) -> pd_client::Result<TikvClient> {
    {
        let clients = tikv_clients.lock().await;
        if let Some(client) = clients.get(&store_id).cloned() {
            return Ok(client);
        }
    }
    let timeout = DEFAULT_CHECK_LEADER_TIMEOUT_DURATION;
    let store = tokio::time::timeout(timeout, pd_client.get_store_async(store_id))
        .await
        .map_err(|e| pd_client::Error::Other(Box::new(e)))
        .flatten()?;
    let mut clients = tikv_clients.lock().await;
    let start = Instant::now_coarse();
    // hack: so it's different args, grpc will always create a new connection.
    // the check leader requests may be large but not frequent, compress it to
    // reduce the traffic.
    let cb = ChannelBuilder::new(env.clone())
        .raw_cfg_int(
            CString::new("random id").unwrap(),
            CONN_ID.fetch_add(1, Ordering::SeqCst),
        )
        .default_compression_algorithm(CompressionAlgorithms::GRPC_COMPRESS_GZIP)
        .default_gzip_compression_level(DEFAULT_GRPC_GZIP_COMPRESSION_LEVEL)
        .default_grpc_min_message_size_to_compress(DEFAULT_GRPC_MIN_MESSAGE_SIZE_TO_COMPRESS);

    let channel = security_mgr.connect(cb, &store.peer_address);
    let cli = TikvClient::new(channel);
    clients.insert(store_id, cli.clone());
    RTS_TIKV_CLIENT_INIT_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());
    Ok(cli)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            mpsc::{channel, Receiver, Sender},
            Arc,
        },
        time::Duration,
    };

    use engine_test::{kv::KvTestEngine, raft::RaftTestEngine};
    use grpcio::{self, ChannelBuilder, EnvBuilder, Server, ServerBuilder};
    use kvproto::{metapb::Region, tikvpb::Tikv, tikvpb_grpc::create_tikv};
    use pd_client::PdClient;
    use raftstore::{
        router::CdcRaftRouter,
        store::{fsm::RaftRouter, util::RegionReadProgress},
    };
    use tikv_util::store::new_peer;

    use super::*;

    type TestCDCHandler = CdcRaftRouter<RaftRouter<KvTestEngine, RaftTestEngine>>;

    #[derive(Clone)]
    struct MockTikv {
        req_tx: Sender<CheckLeaderRequest>,
    }

    impl Tikv for MockTikv {
        fn check_leader(
            &mut self,
            ctx: grpcio::RpcContext<'_>,
            req: CheckLeaderRequest,
            sink: ::grpcio::UnarySink<CheckLeaderResponse>,
        ) {
            self.req_tx.send(req).unwrap();
            ctx.spawn(async {
                sink.success(CheckLeaderResponse::default()).await.unwrap();
            })
        }
    }

    struct MockPdClient {}
    impl PdClient for MockPdClient {}

    fn new_rpc_suite(env: Arc<Environment>) -> (Server, TikvClient, Receiver<CheckLeaderRequest>) {
        let (tx, rx) = channel();
        let tikv_service = MockTikv { req_tx: tx };
        let builder = ServerBuilder::new(env.clone()).register_service(create_tikv(tikv_service));
        let mut server = builder.bind("127.0.0.1", 0).build().unwrap();
        server.start();
        let (_, port) = server.bind_addrs().next().unwrap();
        let addr = format!("127.0.0.1:{}", port);
        let channel = ChannelBuilder::new(env).connect(&addr);
        let client = TikvClient::new(channel);
        (server, client, rx)
    }

    #[tokio::test]
    async fn test_resolve_leader_request_size() {
        let env = Arc::new(EnvBuilder::new().build());
        let (mut server, tikv_client, rx) = new_rpc_suite(env.clone());

        let mut region1 = Region::default();
        region1.id = 1;
        region1.peers.push(new_peer(1, 1));
        region1.peers.push(new_peer(2, 11));
        let progress1 = RegionReadProgress::new(&region1, 1, 1, 1);
        progress1.update_leader_info(1, 1, &region1);

        let mut region2 = Region::default();
        region2.id = 2;
        region2.peers.push(new_peer(1, 2));
        region2.peers.push(new_peer(2, 22));
        let progress2 = RegionReadProgress::new(&region2, 1, 1, 2);
        progress2.update_leader_info(2, 2, &region2);

        let mut leader_resolver = LeadershipResolver::new(
            1, // store id
            Arc::new(MockPdClient {}),
            env.clone(),
            Arc::new(SecurityManager::default()),
            RegionReadProgressRegistry::new(),
            Duration::from_secs(1),
        );
        leader_resolver
            .tikv_clients
            .lock()
            .await
            .insert(2 /* store id */, tikv_client);
        leader_resolver
            .region_read_progress
            .insert(1, Arc::new(progress1));
        leader_resolver
            .region_read_progress
            .insert(2, Arc::new(progress2));

        leader_resolver
            .resolve(
                Either::Left(vec![1, 2]),
                TimeStamp::new(1),
                None as Option<TestCDCHandler>,
            )
            .await;
        let req = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(req.regions.len(), 2);

        // Checking one region only send 1 region in request.
        leader_resolver
            .resolve(
                Either::Left(vec![1]),
                TimeStamp::new(1),
                None as Option<TestCDCHandler>,
            )
            .await;
        let req = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(req.regions.len(), 1);

        // Checking zero region does not send request.
        leader_resolver
            .resolve(
                Either::Left(vec![]),
                TimeStamp::new(1),
                None as Option<TestCDCHandler>,
            )
            .await;
        rx.recv_timeout(Duration::from_secs(1)).unwrap_err();

        let _ = server.shutdown().await;
    }

    #[test]
    fn test_buffer_clear_and_gc() {
        let mut buffer = ResolverBuffer::new(Duration::from_secs(0));
        fn mutate(buffer: &mut ResolverBuffer) {
            let mut check_leader = CheckLeaderRequest::default();
            check_leader.regions.push(LeaderInfo::default());
            check_leader.inactive_regions.push(1);
            check_leader.set_ts(1);
            check_leader.set_store_id(1);
            buffer.store_req_map.insert(1, check_leader);
            buffer.region_map.insert(1, vec![Peer::default()]);
            buffer.resp_map.insert(1, vec![1]);
            buffer.valid_regions.insert(1);
            buffer.store_region_map.insert(1, vec![1]);
            buffer.failed_region_store_map.insert(1, vec![1]);
            buffer.store_unsafe_regions_map.insert(1, vec![1]);
            buffer
                .valid_leader_infos
                .push((LeaderInfo::default(), vec![1]));
            buffer
                .store_checked_leader_map
                .insert(1, vec![CheckedLeader::default()]);
        }

        // clear keep the keys of maps.
        mutate(&mut buffer);
        buffer.clear();
        assert!(
            buffer
                .store_req_map
                .get(&1)
                .unwrap()
                .get_regions()
                .is_empty()
        );
        assert!(
            buffer
                .store_req_map
                .get(&1)
                .unwrap()
                .get_inactive_regions()
                .is_empty()
        );
        assert_eq!(buffer.store_req_map.get(&1).unwrap().get_ts(), 0);
        assert_eq!(buffer.store_req_map.get(&1).unwrap().get_store_id(), 1);
        assert!(
            buffer
                .store_req_map
                .get(&1)
                .unwrap()
                .get_regions()
                .is_empty()
        );
        assert!(
            buffer
                .store_req_map
                .get(&1)
                .unwrap()
                .get_inactive_regions()
                .is_empty()
        );
        assert!(buffer.region_map.get(&1).unwrap().is_empty());
        assert!(buffer.resp_map.get(&1).unwrap().is_empty());
        assert!(buffer.valid_regions.is_empty());
        assert!(buffer.store_region_map.get(&1).unwrap().is_empty());
        assert!(buffer.failed_region_store_map.get(&1).unwrap().is_empty());
        assert!(buffer.store_unsafe_regions_map.get(&1).unwrap().is_empty());
        assert!(buffer.valid_leader_infos.is_empty());
        assert!(buffer.store_checked_leader_map.get(&1).unwrap().is_empty());

        // gc will clear all.
        mutate(&mut buffer);
        buffer.gc();
        assert!(buffer.store_req_map.get(&1).is_none());
        assert!(buffer.region_map.get(&1).is_none());
        assert!(buffer.resp_map.get(&1).is_none());
        assert!(buffer.valid_regions.is_empty());
        assert!(buffer.store_region_map.get(&1).is_none());
        assert!(buffer.failed_region_store_map.get(&1).is_none());
        assert!(buffer.store_unsafe_regions_map.get(&1).is_none());
        assert!(buffer.valid_leader_infos.is_empty());
        assert!(buffer.store_checked_leader_map.get(&1).is_none());
    }
}
