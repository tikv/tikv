// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use futures::compat::Future01CompatExt;
use futures::future::select_all;
use futures::FutureExt;
use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::{CheckLeaderRequest, LeaderInfo};
use kvproto::metapb::{Peer, PeerRole};
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use protobuf::Message;
use raftstore::store::fsm::StoreMeta;
use raftstore::store::util::RegionReadProgressRegistry;
use security::SecurityManager;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::Scheduler;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Mutex;
use txn_types::TimeStamp;

use crate::endpoint::Task;
use crate::errors::Result;
use crate::metrics::{CHECK_LEADER_REQ_ITEM_COUNT_HISTOGRAM, CHECK_LEADER_REQ_SIZE_HISTOGRAM};

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
    #[cfg(feature = "failpoint")]
    (|| fail_point!("before_sync_replica_read_state", |_| regions))();

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
        if find_store_id(&peer_list, leader_id) != Some(store_id) {
            continue;
        }
        for peer in &peer_list {
            if peer.store_id == store_id && peer.id == leader_id {
                resp_map.entry(region_id).or_default().push(store_id);
                if peer_list.len() == 1 {
                    valid_regions.insert(region_id);
                }
                continue;
            }
            store_map
                .entry(peer.store_id)
                .or_default()
                .push(leader_info.clone());
        }
        region_map.insert(region_id, peer_list);
    }
    // Approximate `LeaderInfo` size
    let leader_info_size = store_map
        .values()
        .next()
        .map_or(0, |regions| regions[0].compute_size());
    let store_count = store_map.len();
    let mut stores: Vec<_> = store_map
        .into_iter()
        .map(|(store_id, regions)| {
            let tikv_clients = tikv_clients.clone();
            let env = env.clone();
            let pd_client = pd_client.clone();
            let security_mgr = security_mgr.clone();
            let region_num = regions.len() as u32;
            CHECK_LEADER_REQ_SIZE_HISTOGRAM.observe((leader_info_size * region_num) as f64);
            CHECK_LEADER_REQ_ITEM_COUNT_HISTOGRAM.observe(region_num as f64);
            async move {
                let client = box_try!(
                    get_tikv_client(store_id, pd_client, security_mgr, env, tikv_clients.clone())
                        .await
                );
                let mut req = CheckLeaderRequest::default();
                req.set_regions(regions.into());
                req.set_ts(min_ts.into_inner());
                let res = box_try!(
                    tokio::time::timeout(
                        Duration::from_millis(DEFAULT_CHECK_LEADER_TIMEOUT_MILLISECONDS),
                        box_try!(client.check_leader_async(&req))
                    )
                    .await
                );
                let resp = match res {
                    Ok(resp) => resp,
                    Err(err) => {
                        tikv_clients.lock().await.remove(&store_id);
                        return Err(box_err!(err));
                    }
                };
                Result::Ok((store_id, resp))
            }
            .boxed()
        })
        .collect();
    for _ in 0..store_count {
        // Use `select_all` to avoid the process getting blocked when some TiKVs were down.
        let (res, _, remains) = select_all(stores).await;
        stores = remains;
        if let Ok((store_id, resp)) = res {
            for region_id in resp.regions {
                resp_map.entry(region_id).or_default().push(store_id);
                if region_has_quorum(&region_map[&region_id], &resp_map[&region_id]) {
                    valid_regions.insert(region_id);
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

fn find_store_id(peer_list: &[Peer], peer_id: u64) -> Option<u64> {
    for peer in peer_list {
        if peer.id == peer_id {
            return Some(peer.store_id);
        }
    }
    None
}

async fn get_tikv_client(
    store_id: u64,
    pd_client: Arc<dyn PdClient>,
    security_mgr: Arc<SecurityManager>,
    env: Arc<Environment>,
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
) -> Result<TikvClient> {
    let mut clients = tikv_clients.lock().await;
    let client = match clients.get(&store_id) {
        Some(client) => client.clone(),
        None => {
            let store = box_try!(pd_client.get_store_async(store_id).await);
            let cb = ChannelBuilder::new(env.clone());
            let channel = security_mgr.connect(cb, &store.address);
            let client = TikvClient::new(channel);
            clients.insert(store_id, client.clone());
            client
        }
    };
    Ok(client)
}
