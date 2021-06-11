// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use futures::compat::Future01CompatExt;
use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::{CheckLeaderRequest, LeaderInfo};
use kvproto::metapb::{PeerRole, Region};
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use protobuf::Message;
use raftstore::store::fsm::StoreMeta;
use raftstore::store::util::RegionReadProgressRegistry;
use security::SecurityManager;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::Scheduler;
use tokio::runtime::{Builder, Runtime};
use txn_types::TimeStamp;

use crate::endpoint::Task;
use crate::errors::Result;
use crate::metrics::CHECK_LEADER_REQ_SIZE_HISTOGRAM;

pub struct AdvanceTsWorker<E: KvEngine> {
    store_meta: Arc<Mutex<StoreMeta>>,
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
        store_meta: Arc<Mutex<StoreMeta>>,
        region_read_progress: RegionReadProgressRegistry,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        let worker = Builder::new_multi_thread()
            .thread_name("advance-ts")
            .worker_threads(1)
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
    pub fn register_advance_event(&self, advance_ts_interval: Duration, regions: Vec<u64>) {
        let timeout = self.timer.delay(advance_ts_interval);
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let cm: ConcurrencyManager = self.concurrency_manager.clone();
        let env = self.env.clone();
        let security_mgr = self.security_mgr.clone();
        let store_meta = self.store_meta.clone();
        let tikv_clients = self.tikv_clients.clone();
        let region_read_progress = self.region_read_progress.clone();

        let fut = async move {
            let _ = timeout.compat().await;
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

            if let Err(e) = scheduler.schedule(Task::RegisterAdvanceEvent) {
                info!("failed to schedule register advance event"; "err" => ?e);
            }

            let regions = Self::region_resolved_ts_store(
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

    // Confirms leadership of region peer before trying to advance resolved ts.
    // This function broadcasts a special message to all stores, get the leader id of them to confirm whether
    // current peer has a quorum which accept its leadership.
    async fn region_resolved_ts_store(
        regions: Vec<u64>,
        store_meta: Arc<Mutex<StoreMeta>>,
        region_read_progress: RegionReadProgressRegistry,
        pd_client: Arc<dyn PdClient>,
        security_mgr: Arc<SecurityManager>,
        env: Arc<Environment>,
        cdc_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
        min_ts: TimeStamp,
    ) -> Vec<u64> {
        #[cfg(feature = "failpoint")]
        (|| fail_point!("before_sync_replica_read_state", |_| regions))();

        // store_id -> leaders info, record the request to each stores
        let mut store_map: HashMap<u64, Vec<LeaderInfo>> = HashMap::default();
        // region_id -> region, cache the information of regions
        let mut region_map: HashMap<u64, Region> = HashMap::default();
        // region_id -> peers id, record the responses
        let mut resp_map: HashMap<u64, Vec<u64>> = HashMap::default();
        // region_id -> `read_state`, getting the `read_state` first to reduce
        // the time of holding the `store_meta` mutex
        let mut read_state_map = region_read_progress.get_read_state(&regions);
        {
            let meta = store_meta.lock().unwrap();
            let store_id = match meta.store_id {
                Some(id) => id,
                None => return vec![],
            };
            for region_id in regions {
                if let Some(region) = meta.regions.get(&region_id) {
                    if let Some((term, leader_id)) = meta.leaders.get(&region_id) {
                        let leader_store_id = find_store_id(&region, *leader_id);
                        if leader_store_id.is_none() {
                            continue;
                        }
                        if leader_store_id.unwrap() != meta.store_id.unwrap() {
                            continue;
                        }
                        let read_state = read_state_map.remove(&region_id);
                        for peer in region.get_peers() {
                            if peer.store_id == store_id && peer.id == *leader_id {
                                resp_map.entry(region_id).or_default().push(store_id);
                                continue;
                            }
                            let mut leader_info = LeaderInfo::default();
                            leader_info.set_peer_id(*leader_id);
                            leader_info.set_term(*term);
                            leader_info.set_region_id(region_id);
                            leader_info.set_region_epoch(region.get_region_epoch().clone());
                            if let Some(rs) = &read_state {
                                leader_info.set_read_state(rs.clone());
                            }
                            store_map
                                .entry(peer.store_id)
                                .or_default()
                                .push(leader_info);
                        }
                        region_map.insert(region_id, region.clone());
                    }
                }
            }
        }
        let stores = store_map.into_iter().map(|(store_id, regions)| {
            let cdc_clients = cdc_clients.clone();
            let env = env.clone();
            let pd_client = pd_client.clone();
            let security_mgr = security_mgr.clone();
            async move {
                if cdc_clients.lock().unwrap().get(&store_id).is_none() {
                    let store = box_try!(pd_client.get_store_async(store_id).await);
                    let cb = ChannelBuilder::new(env.clone());
                    let channel = security_mgr.connect(cb, &store.address);
                    cdc_clients
                        .lock()
                        .unwrap()
                        .insert(store_id, TikvClient::new(channel));
                }
                let client = cdc_clients.lock().unwrap().get(&store_id).unwrap().clone();
                let mut req = CheckLeaderRequest::default();
                req.set_regions(regions.into());
                req.set_ts(min_ts.into_inner());
                // TODO: maybe should compute request size by len * `LeaderInfo::compute_size`
                CHECK_LEADER_REQ_SIZE_HISTOGRAM.observe(req.compute_size() as f64);
                let res = box_try!(client.check_leader_async(&req)).await;
                let resp = box_try!(res);
                Result::Ok((store_id, resp))
            }
        });
        let resps = futures::future::join_all(stores).await;
        resps
            .into_iter()
            .filter_map(|resp| match resp {
                Ok(resp) => Some(resp),
                Err(e) => {
                    debug!("resolved-ts check leader error"; "err" =>?e);
                    None
                }
            })
            .map(|(store_id, resp)| {
                resp.regions
                    .into_iter()
                    .map(move |region_id| (store_id, region_id))
            })
            .flatten()
            .for_each(|(store_id, region_id)| {
                resp_map.entry(region_id).or_default().push(store_id);
            });
        resp_map
            .into_iter()
            .filter_map(|(region_id, stores)| {
                if region_has_quorum(&region_map[&region_id], &stores) {
                    Some(region_id)
                } else {
                    debug!(
                        "resolved-ts cannot get quorum for resolved ts";
                        "region_id" => region_id,
                        "stores" => ?stores,
                        "region" => ?&region_map[&region_id]
                    );
                    None
                }
            })
            .collect()
    }
}

fn region_has_quorum(region: &Region, stores: &[u64]) -> bool {
    let mut voters = 0;
    let mut incoming_voters = 0;
    let mut demoting_voters = 0;

    let mut resp_voters = 0;
    let mut resp_incoming_voters = 0;
    let mut resp_demoting_voters = 0;

    region.get_peers().iter().for_each(|peer| {
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

fn find_store_id(region: &Region, peer_id: u64) -> Option<u64> {
    for peer in region.get_peers() {
        if peer.id == peer_id {
            return Some(peer.store_id);
        }
    }
    None
}
