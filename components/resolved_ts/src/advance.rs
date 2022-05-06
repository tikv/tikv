// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ffi::CString,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use fail::fail_point;
use futures::{compat::Future01CompatExt, future::select_all, FutureExt, TryFutureExt};
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{CheckLeaderRequest, LeaderInfo},
    metapb::{Peer, PeerRole},
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use protobuf::Message;
use raftstore::store::{fsm::StoreMeta, util::RegionReadProgressRegistry};
use security::SecurityManager;
use tikv_util::{info, time::Instant, timer::SteadyTimer, worker::Scheduler};
use tokio::{
    runtime::{Builder, Runtime},
    sync::Mutex,
};
use txn_types::TimeStamp;

use crate::{endpoint::Task, errors::Result, metrics::*, util};

const DEFAULT_CHECK_LEADER_TIMEOUT_MILLISECONDS: u64 = 5_000; // 5s

pub struct AdvanceTsWorker<E: KvEngine> {
    store_meta: Arc<StdMutex<StoreMeta>>,
    region_read_progress: RegionReadProgressRegistry,
    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    worker: Runtime,
    scheduler: Scheduler<Task<E::Snapshot>>,
    /// The concurrency manager for transactions. It's needed for CDC to check locks when
    /// calculating resolved_ts.
    concurrency_manager: ConcurrencyManager,
    // store_id -> client
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

impl<E: KvEngine> AdvanceTsWorker<E> {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task<E::Snapshot>>,
        store_meta: Arc<StdMutex<StoreMeta>>,
        region_read_progress: RegionReadProgressRegistry,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        let worker = Builder::new_multi_thread()
            .thread_name("advance-ts")
            .worker_threads(1)
            .enable_time()
            .build()
            .unwrap();
        Self {
            env,
            security_mgr,
            scheduler,
            pd_client,
            worker,
            timer: SteadyTimer::default(),
            store_meta,
            region_read_progress,
            concurrency_manager,
            tikv_clients: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

impl<E: KvEngine> AdvanceTsWorker<E> {
    pub fn advance_ts_for_regions(&self, regions: Vec<u64>) {
        if regions.is_empty() {
            return;
        }
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let cm: ConcurrencyManager = self.concurrency_manager.clone();
        let env = self.env.clone();
        let security_mgr = self.security_mgr.clone();
        let store_meta = self.store_meta.clone();
        let tikv_clients = self.tikv_clients.clone();
        let region_read_progress = self.region_read_progress.clone();

        let fut = async move {
            // Ignore get tso errors since we will retry every `advance_ts_interval`.
            let mut min_ts = pd_client.get_tso().await.unwrap_or_default();

            // Sync with concurrency manager so that it can work correctly when optimizations
            // like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTS` task, and the
            // resolver must be checked in or after `Task::MinTS`' execution.
            cm.update_max_ts(min_ts);
            if let Some(min_mem_lock_ts) = cm.global_min_lock_ts() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                }
            }

            let regions = region_resolved_ts_store(
                regions,
                store_meta,
                region_read_progress,
                pd_client,
                security_mgr,
                env,
                tikv_clients,
                min_ts,
            )
            .await;

            if !regions.is_empty() {
                if let Err(e) = scheduler.schedule(Task::AdvanceResolvedTs {
                    regions,
                    ts: min_ts,
                }) {
                    info!("failed to schedule advance event"; "err" => ?e);
                }
            }
        };
        self.worker.spawn(fut);
    }

    pub fn register_next_event(&self, advance_ts_interval: Duration, cfg_version: usize) {
        let scheduler = self.scheduler.clone();
        let timeout = self.timer.delay(advance_ts_interval);
        let fut = async move {
            let _ = timeout.compat().await;
            if let Err(e) = scheduler.schedule(Task::RegisterAdvanceEvent { cfg_version }) {
                info!("failed to schedule register advance event"; "err" => ?e);
            }
        };
        self.worker.spawn(fut);
    }
}

// Confirms leadership of region peer before trying to advance resolved ts.
// This function broadcasts a special message to all stores, gets the leader id of them to confirm whether
// current peer has a quorum which accepts its leadership.
pub async fn region_resolved_ts_store(
    regions: Vec<u64>,
    store_meta: Arc<StdMutex<StoreMeta>>,
    region_read_progress: RegionReadProgressRegistry,
    pd_client: Arc<dyn PdClient>,
    security_mgr: Arc<SecurityManager>,
    env: Arc<Environment>,
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
    min_ts: TimeStamp,
) -> Vec<u64> {
    PENDING_RTS_COUNT.inc();
    defer!(PENDING_RTS_COUNT.dec());
    fail_point!("before_sync_replica_read_state", |_| regions.clone());

    let store_id = match store_meta.lock().unwrap().store_id {
        Some(id) => id,
        None => return vec![],
    };

    // store_id -> leaders info, record the request to each stores
    let mut store_map: HashMap<u64, Vec<LeaderInfo>> = HashMap::default();
    // region_id -> region, cache the information of regions
    let mut region_map: HashMap<u64, Vec<Peer>> = HashMap::default();
    // region_id -> peers id, record the responses
    let mut resp_map: HashMap<u64, Vec<u64>> = HashMap::default();
    // region_id -> `(Vec<Peer>, LeaderInfo)`
    let info_map = region_read_progress.dump_leader_infos(&regions);
    let mut valid_regions = HashSet::default();

    for (region_id, (peer_list, leader_info)) in info_map {
        let leader_id = leader_info.get_peer_id();
        // Check if the leader in this store
        if util::find_store_id(&peer_list, leader_id) != Some(store_id) {
            continue;
        }
        let mut unvotes = 0;
        for peer in &peer_list {
            if peer.store_id == store_id && peer.id == leader_id {
                resp_map.entry(region_id).or_default().push(store_id);
            } else {
                // It's still necessary to check leader on learners even if they don't vote
                // because performing stale read on learners require it.
                store_map
                    .entry(peer.store_id)
                    .or_default()
                    .push(leader_info.clone());
                if peer.get_role() != PeerRole::Learner {
                    unvotes += 1;
                }
            }
        }
        // Check `region_has_quorum` here because `store_map` can be empty,
        // in which case `region_has_quorum` won't be called any more.
        if unvotes == 0 && region_has_quorum(&peer_list, &resp_map[&region_id]) {
            valid_regions.insert(region_id);
        } else {
            region_map.insert(region_id, peer_list);
        }
    }
    // Approximate `LeaderInfo` size
    let leader_info_size = store_map
        .values()
        .next()
        .map_or(0, |regions| regions[0].compute_size());
    let store_count = store_map.len();
    let mut stores: Vec<_> = store_map
        .into_iter()
        .map(|(to_store, regions)| {
            let tikv_clients = tikv_clients.clone();
            let env = env.clone();
            let pd_client = pd_client.clone();
            let security_mgr = security_mgr.clone();
            let region_num = regions.len() as u32;
            CHECK_LEADER_REQ_SIZE_HISTOGRAM.observe((leader_info_size * region_num) as f64);
            CHECK_LEADER_REQ_ITEM_COUNT_HISTOGRAM.observe(region_num as f64);

            // Check leadership for `regions` on `to_store`.
            async move {
                PENDING_CHECK_LEADER_REQ_COUNT.inc();
                defer!(PENDING_CHECK_LEADER_REQ_COUNT.dec());
                let client =
                    get_tikv_client(to_store, pd_client, security_mgr, env, tikv_clients.clone())
                        .await
                        .map_err(|e| (to_store, true, format!("[get tikv client] {}", e)))?;

                let mut req = CheckLeaderRequest::default();
                req.set_regions(regions.into());
                req.set_ts(min_ts.into_inner());
                let start = Instant::now_coarse();
                defer!({
                    let elapsed = start.saturating_elapsed();
                    slow_log!(
                        elapsed,
                        "check leader rpc costs too long, to_store: {}",
                        to_store
                    );
                    RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC
                        .with_label_values(&["rpc"])
                        .observe(elapsed.as_secs_f64());
                });

                let rpc = client
                    .check_leader_async(&req)
                    .map_err(|e| (to_store, true, format!("[rpc create failed]{}", e)))?;
                PENDING_CHECK_LEADER_REQ_SENT_COUNT.inc();
                defer!(PENDING_CHECK_LEADER_REQ_SENT_COUNT.dec());
                let timeout = Duration::from_millis(DEFAULT_CHECK_LEADER_TIMEOUT_MILLISECONDS);
                let resp = tokio::time::timeout(timeout, rpc)
                    .map_err(|e| (to_store, true, format!("[timeout] {}", e)))
                    .await?
                    .map_err(|e| (to_store, true, format!("[rpc failed] {}", e)))?;
                Ok((to_store, resp))
            }
            .boxed()
        })
        .collect();
    let start = Instant::now_coarse();

    defer!({
        RTS_CHECK_LEADER_DURATION_HISTOGRAM_VEC
            .with_label_values(&["all"])
            .observe(start.saturating_elapsed_secs());
    });
    for _ in 0..store_count {
        // Use `select_all` to avoid the process getting blocked when some TiKVs were down.
        let (res, _, remains) = select_all(stores).await;
        stores = remains;
        match res {
            Ok((to_store, resp)) => {
                for region_id in resp.regions {
                    if let Some(r) = region_map.get(&region_id) {
                        let resps = resp_map.entry(region_id).or_default();
                        resps.push(to_store);
                        if region_has_quorum(r, resps) {
                            valid_regions.insert(region_id);
                        }
                    }
                }
            }
            Err((to_store, reconnect, err)) => {
                info!("check leader failed"; "error" => ?err, "to_store" => to_store);
                if reconnect {
                    tikv_clients.lock().await.remove(&to_store);
                }
            }
        }
        // Return early if all regions had already got quorum.
        if valid_regions.len() == regions.len() {
            // break here because all regions have quorum,
            // so there is no need waiting for other stores to respond.
            break;
        }
    }
    valid_regions.into_iter().collect()
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
    pd_client: Arc<dyn PdClient>,
    security_mgr: Arc<SecurityManager>,
    env: Arc<Environment>,
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
) -> Result<TikvClient> {
    {
        let clients = tikv_clients.lock().await;
        if let Some(client) = clients.get(&store_id).cloned() {
            return Ok(client);
        }
    }
    let timeout = Duration::from_millis(DEFAULT_CHECK_LEADER_TIMEOUT_MILLISECONDS);
    let store = box_try!(box_try!(
        tokio::time::timeout(timeout, pd_client.get_store_async(store_id)).await
    ));
    let mut clients = tikv_clients.lock().await;
    let start = Instant::now_coarse();
    // hack: so it's different args, grpc will always create a new connection.
    let cb = ChannelBuilder::new(env.clone()).raw_cfg_int(
        CString::new("random id").unwrap(),
        CONN_ID.fetch_add(1, Ordering::SeqCst),
    );
    let channel = security_mgr.connect(cb, &store.address);
    let cli = TikvClient::new(channel);
    clients.insert(store_id, cli.clone());
    RTS_TIKV_CLIENT_INIT_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());
    Ok(cli)
}
