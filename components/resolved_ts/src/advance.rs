// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    ffi::CString,
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
    kvrpcpb::{CheckLeaderRequest, CheckLeaderResponse},
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
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{Mutex, Notify},
};
use txn_types::TimeStamp;

use crate::{endpoint::Task, metrics::*, TsSource};

pub(crate) const DEFAULT_CHECK_LEADER_TIMEOUT_DURATION: Duration = Duration::from_secs(5); // 5s
const DEFAULT_GRPC_GZIP_COMPRESSION_LEVEL: usize = 2;
const DEFAULT_GRPC_MIN_MESSAGE_SIZE_TO_COMPRESS: usize = 4096;

pub struct AdvanceTsWorker {
    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    worker: Runtime,
    scheduler: Scheduler<Task>,
    /// The concurrency manager for transactions. It's needed for CDC to check
    /// locks when calculating resolved_ts.
    pub(crate) concurrency_manager: ConcurrencyManager,

    // cache the last pd tso, used to approximate the next timestamp w/o an actual TSO RPC
    pub(crate) last_pd_tso: Arc<std::sync::Mutex<Option<(TimeStamp, Instant)>>>,
}

impl AdvanceTsWorker {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        concurrency_manager: ConcurrencyManager,
    ) -> Self {
        let worker = Builder::new_multi_thread()
            .thread_name("advance-ts")
            .worker_threads(1)
            .enable_time()
            .with_sys_hooks()
            .build()
            .unwrap();
        Self {
            scheduler,
            pd_client,
            worker,
            timer: SteadyTimer::default(),
            concurrency_manager,
            last_pd_tso: Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

impl AdvanceTsWorker {
    // Advance ts asynchronously and register RegisterAdvanceEvent when its done.
    pub fn advance_ts_for_regions(
        &self,
        regions: Vec<u64>,
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
            advance_ts_interval,
        ));

        let last_pd_tso = self.last_pd_tso.clone();
        let fut = async move {
            // Ignore get tso errors since we will retry every `advdance_ts_interval`.
            let mut min_ts = pd_client.get_tso().await.unwrap_or_default();
            if let Ok(mut last_pd_tso) = last_pd_tso.try_lock() {
                *last_pd_tso = Some((min_ts, Instant::now()));
            }
            let mut ts_source = TsSource::PdTso;

            // Sync with concurrency manager so that it can work correctly when
            // optimizations like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTs` task, and the
            // resolver must be checked in or after `Task::MinTs`' execution.
            cm.update_max_ts(min_ts, Some("resolved-ts".to_owned()))
                .unwrap();
            if let Some((min_mem_lock_ts, lock)) = cm.global_min_lock() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                    ts_source = TsSource::MemoryLock(lock);
                }
            }

            let regions = leader_resolver
                .resolve(regions, min_ts, Some(advance_ts_interval))
                .await;
            if !regions.is_empty() {
                if let Err(e) = scheduler.schedule(Task::ResolvedTsAdvanced {
                    regions,
                    ts: min_ts,
                    ts_source,
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

    // store_id -> check leader request, record the request to each stores.
    store_req_map: HashMap<u64, CheckLeaderRequest>,
    progresses: HashMap<u64, RegionProgress>,
    checking_regions: HashSet<u64>,
    valid_regions: HashSet<u64>,

    gc_interval: Duration,
    last_gc_time: Instant,
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

            store_req_map: HashMap::default(),
            progresses: HashMap::default(),
            valid_regions: HashSet::default(),
            checking_regions: HashSet::default(),
            last_gc_time: Instant::now_coarse(),
            gc_interval,
        }
    }

    fn gc(&mut self) {
        let now = Instant::now_coarse();
        if now - self.last_gc_time > self.gc_interval {
            self.store_req_map = HashMap::default();
            self.progresses = HashMap::default();
            self.valid_regions = HashSet::default();
            self.checking_regions = HashSet::default();
            self.last_gc_time = now;
        }
    }

    fn clear(&mut self) {
        for v in self.store_req_map.values_mut() {
            v.regions.clear();
            v.ts = 0;
        }
        for v in self.progresses.values_mut() {
            v.clear();
        }
        self.checking_regions.clear();
        self.valid_regions.clear();
    }

    // Confirms leadership of region peer before trying to advance resolved ts.
    // This function broadcasts a special message to all stores, gets the leader id
    // of them to confirm whether current peer has a quorum which accepts its
    // leadership.
    pub async fn resolve(
        &mut self,
        regions: Vec<u64>,
        min_ts: TimeStamp,
        timeout: Option<Duration>,
    ) -> Vec<u64> {
        if regions.is_empty() {
            return regions;
        }

        // Clear previous result before resolving.
        self.clear();
        // GC when necessary to prevent memory leak.
        self.gc();

        PENDING_RTS_COUNT.inc();
        defer!(PENDING_RTS_COUNT.dec());
        fail_point!("before_sync_replica_read_state", |_| regions.clone());

        let store_id = self.store_id;
        let valid_regions = &mut self.valid_regions;
        let progresses = &mut self.progresses;
        let store_req_map = &mut self.store_req_map;
        let checking_regions = &mut self.checking_regions;
        for region_id in &regions {
            checking_regions.insert(*region_id);
        }
        self.region_read_progress.with(|registry| {
            for (region_id, read_progress) in registry {
                if !checking_regions.contains(region_id) {
                    continue;
                }
                let core = read_progress.get_core();
                let local_leader_info = core.get_local_leader_info();
                let leader_id = local_leader_info.get_leader_id();
                let leader_store_id = local_leader_info.get_leader_store_id();
                let peer_list = local_leader_info.get_peers();
                // Check if the leader in this store
                if leader_store_id != Some(store_id) {
                    continue;
                }
                let leader_info = core.get_leader_info();

                let prog = progresses
                    .entry(*region_id)
                    .or_insert_with(|| RegionProgress::new(peer_list.len()));
                let mut unvotes = 0;
                for peer in peer_list {
                    if peer.store_id == store_id && peer.id == leader_id {
                        prog.resps.push(store_id);
                    } else {
                        // It's still necessary to check leader on learners even if they don't vote
                        // because performing stale read on learners require it.
                        store_req_map
                            .entry(peer.store_id)
                            .or_insert_with(|| {
                                let mut req = CheckLeaderRequest::default();
                                req.regions = Vec::with_capacity(registry.len()).into();
                                req
                            })
                            .regions
                            .push(leader_info.clone());
                        if peer.get_role() != PeerRole::Learner {
                            unvotes += 1;
                        }
                    }
                }

                // Check `region_has_quorum` here because `store_map` can be empty,
                // in which case `region_has_quorum` won't be called any more.
                if unvotes == 0 && region_has_quorum(peer_list, &prog.resps) {
                    prog.resolved = true;
                    valid_regions.insert(*region_id);
                } else {
                    prog.peers.extend_from_slice(peer_list);
                }
            }
        });

        let env = &self.env;
        let pd_client = &self.pd_client;
        let security_mgr = &self.security_mgr;
        let tikv_clients = &self.tikv_clients;
        // Approximate `LeaderInfo` size
        let leader_info_size = store_req_map
            .values()
            .find(|req| !req.regions.is_empty())
            .map_or(0, |req| req.regions[0].compute_size());
        let mut check_leader_rpcs = Vec::with_capacity(store_req_map.len());
        let timeout = get_min_timeout(timeout, DEFAULT_CHECK_LEADER_TIMEOUT_DURATION);

        for (store_id, req) in store_req_map {
            if req.regions.is_empty() {
                continue;
            }
            let env = env.clone();
            let to_store = *store_id;
            let region_num = req.regions.len() as u32;
            CHECK_LEADER_REQ_SIZE_HISTOGRAM.observe((leader_info_size * region_num) as f64);
            CHECK_LEADER_REQ_ITEM_COUNT_HISTOGRAM.observe(region_num as f64);

            // Check leadership for `regions` on `to_store`.
            let rpc = async move {
                PENDING_CHECK_LEADER_REQ_COUNT.inc();
                defer!(PENDING_CHECK_LEADER_REQ_COUNT.dec());
                let client = get_tikv_client(
                    to_store,
                    pd_client,
                    security_mgr,
                    env,
                    tikv_clients,
                    timeout,
                )
                .await
                .map_err(|e| (to_store, e.retryable(), format!("[get tikv client] {}", e)))?;

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
                    Err(GrpcError::RpcFailure(status))
                        if status.code() == RpcStatusCode::UNIMPLEMENTED =>
                    {
                        // Some stores like TiFlash don't implement it.
                        return Ok((to_store, CheckLeaderResponse::default()));
                    }
                    Err(e) => return Err((to_store, true, format!("[rpc create failed]{}", e))),
                };

                PENDING_CHECK_LEADER_REQ_SENT_COUNT.inc();
                defer!(PENDING_CHECK_LEADER_REQ_SENT_COUNT.dec());
                let resp = tokio::time::timeout(timeout, rpc)
                    .map_err(|e| (to_store, true, format!("[timeout] {}", e)))
                    .await?
                    .map_err(|e| (to_store, true, format!("[rpc failed] {}", e)))?;
                Ok((to_store, resp))
            }
            .boxed();
            check_leader_rpcs.push(rpc);
        }
        let start = Instant::now_coarse();

        defer!({
            RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC
                .with_label_values(&["all"])
                .observe(start.saturating_elapsed_secs());
        });

        let rpc_count = check_leader_rpcs.len();
        for _ in 0..rpc_count {
            // Use `select_all` to avoid the process getting blocked when some
            // TiKVs were down.
            let (res, _, remains) = select_all(check_leader_rpcs).await;
            check_leader_rpcs = remains;
            match res {
                Ok((to_store, resp)) => {
                    for region_id in resp.regions {
                        if let Some(prog) = progresses.get_mut(&region_id) {
                            if prog.resolved {
                                continue;
                            }
                            prog.resps.push(to_store);
                            if region_has_quorum(&prog.peers, &prog.resps) {
                                prog.resolved = true;
                                valid_regions.insert(region_id);
                            }
                        }
                    }
                }
                Err((to_store, reconnect, err)) => {
                    info!("check leader failed"; "error" => ?err, "to_store" => to_store);
                    if reconnect {
                        self.tikv_clients.lock().await.remove(&to_store);
                    }
                }
            }
            if valid_regions.len() >= progresses.len() {
                break;
            }
        }
        let res: Vec<u64> = self.valid_regions.drain().collect();
        if res.len() != checking_regions.len() {
            warn!(
                "check leader returns valid regions different from checking regions";
                "valid_regions" => res.len(),
                "checking_regions" => checking_regions.len(),
            );
        }
        res
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

#[inline]
fn get_min_timeout(timeout: Option<Duration>, default: Duration) -> Duration {
    timeout.unwrap_or(default).min(default)
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
    timeout: Duration,
) -> pd_client::Result<TikvClient> {
    {
        let clients = tikv_clients.lock().await;
        if let Some(client) = clients.get(&store_id).cloned() {
            return Ok(client);
        }
    }
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

struct RegionProgress {
    resolved: bool,
    peers: Vec<Peer>,
    resps: Vec<u64>,
}

impl RegionProgress {
    fn new(len: usize) -> Self {
        RegionProgress {
            resolved: false,
            peers: Vec::with_capacity(len),
            resps: Vec::with_capacity(len),
        }
    }
    fn clear(&mut self) {
        self.resolved = false;
        self.peers.clear();
        self.resps.clear();
    }
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

    use grpcio::{self, ChannelBuilder, EnvBuilder, Server, ServerBuilder};
    use kvproto::{metapb::Region, tikvpb::Tikv, tikvpb_grpc::create_tikv};
    use pd_client::PdClient;
    use raftstore::store::util::RegionReadProgress;
    use tikv_util::store::new_peer;

    use super::*;

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
            .resolve(vec![1, 2], TimeStamp::new(1), None)
            .await;
        let req = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(req.regions.len(), 2);

        // Checking one region only send 1 region in request.
        leader_resolver
            .resolve(vec![1], TimeStamp::new(1), None)
            .await;
        let req = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(req.regions.len(), 1);

        // Checking zero region does not send request.
        leader_resolver
            .resolve(vec![], TimeStamp::new(1), None)
            .await;
        rx.recv_timeout(Duration::from_secs(1)).unwrap_err();

        let _ = server.shutdown().await;
    }

    #[test]
    fn test_get_min_timeout() {
        assert_eq!(
            get_min_timeout(None, Duration::from_secs(5)),
            Duration::from_secs(5)
        );
        assert_eq!(
            get_min_timeout(None, Duration::from_secs(2)),
            Duration::from_secs(2)
        );
        assert_eq!(
            get_min_timeout(Some(Duration::from_secs(1)), Duration::from_secs(5)),
            Duration::from_secs(1)
        );
        assert_eq!(
            get_min_timeout(Some(Duration::from_secs(20)), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }
}
