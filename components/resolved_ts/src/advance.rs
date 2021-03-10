use std::sync::{Arc, Mutex};
use std::time::Duration;

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use futures::compat::Future01CompatExt;
use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::{CheckLeaderRequest, LeaderInfo, ReadState};
use kvproto::metapb::{PeerRole, Region, RegionEpoch};
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::store::{RegionReadProgress, RegionSafeTSTracker};
use raftstore::store::fsm::StoreMeta;
use raftstore::store::msg::{Callback, SignificantMsg};
use security::SecurityManager;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::Scheduler;
use tokio::runtime::{Builder, Runtime};
use txn_types::TimeStamp;

use crate::endpoint::Task;
use crate::errors::Result;

pub struct AdvanceTsWorker<T, E: KvEngine> {
    store_meta: Arc<Mutex<StoreMeta>>,
    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    worker: Runtime,
    raft_router: T,
    advance_ts_interval: Duration,
    scheduler: Scheduler<Task<E::Snapshot>>,
    /// The concurrency manager for transactions. It's needed for CDC to check locks when
    /// calculating resolved_ts.
    concurrency_manager: ConcurrencyManager,
    hibernate_regions_compatible: bool,
    // store_id -> client
    tikv_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

impl<T, E: KvEngine> AdvanceTsWorker<T, E> {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task<E::Snapshot>>,
        raft_router: T,
        store_meta: Arc<Mutex<StoreMeta>>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        advance_ts_interval: Duration,
        hibernate_regions_compatible: bool,
    ) -> Self {
        let worker = Builder::new()
            .threaded_scheduler()
            .thread_name("advance-ts")
            .core_threads(1)
            .build()
            .unwrap();
        Self {
            env,
            security_mgr,
            scheduler,
            pd_client,
            worker,
            timer: SteadyTimer::default(),
            raft_router,
            store_meta,
            concurrency_manager,
            advance_ts_interval,
            hibernate_regions_compatible: hibernate_regions_compatible,
            tikv_clients: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> AdvanceTsWorker<T, E> {
    pub fn register_advance_event(&self, regions: Vec<u64>) {
        let timeout = self.timer.delay(self.advance_ts_interval);
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let raft_router = self.raft_router.clone();
        let cm: ConcurrencyManager = self.concurrency_manager.clone();
        let env = self.env.clone();
        let security_mgr = self.security_mgr.clone();
        let store_meta = self.store_meta.clone();
        let tikv_clients = self.tikv_clients.clone();
        let hibernate_regions_compatible = self.hibernate_regions_compatible;

        let fut = async move {
            let _ = timeout.compat().await;
            // Ignore get tso errors since we will retry every `min_ts_interval`.
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
                info!(""; "err" => ?e);
            }

            let regions = if hibernate_regions_compatible {
                Self::region_resolved_ts_store(
                    regions,
                    store_meta,
                    pd_client,
                    security_mgr,
                    env,
                    tikv_clients,
                    min_ts,
                )
                .await
            } else {
                Self::region_resolved_ts_raft(regions, store_meta, raft_router, min_ts).await
            };

            if !regions.is_empty() {
                if let Err(e) = scheduler.schedule(Task::AdvanceResolvedTs {
                    regions,
                    ts: min_ts,
                }) {
                    info!(""; "err" => ?e);
                }
            }
        };
        self.worker.spawn(fut);
    }

    async fn region_resolved_ts_raft(
        regions: Vec<u64>,
        store_meta: Arc<Mutex<StoreMeta>>,
        raft_router: T,
        min_ts: TimeStamp,
    ) -> Vec<(u64, RegionEpoch)> {
        let regions: Vec<_> = {
            let meta = store_meta.lock().unwrap();
            regions
                .into_iter()
                .filter_map(|region_id| {
                    meta.regions
                        .get(&region_id)
                        .map(|region| (region_id, region.get_region_epoch().clone()))
                })
                .collect()
        };
        // TODO: send a message to raftstore would consume too much cpu time,
        // try to handle it outside raftstore.
        let regions: Vec<_> = regions
            .into_iter()
            .map(|(region_id, epoch)| {
                let raft_router_clone = raft_router.clone();
                async move {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    if let Err(e) = raft_router_clone.significant_send(
                        region_id,
                        SignificantMsg::LeaderCallback(Callback::Read(Box::new(move |resp| {
                            let resp = if resp.response.get_header().has_error() {
                                None
                            } else {
                                Some((region_id, epoch))
                            };
                            if tx.send(resp).is_err() {
                                error!("cdc send tso response failed"; "region_id" => region_id);
                            }
                        }))),
                    ) {
                        warn!("cdc send LeaderCallback failed"; "err" => ?e, "min_ts" => min_ts);
                        return None;
                    }
                    rx.await.unwrap_or(None)
                }
            })
            .collect();
        let resps = futures::future::join_all(regions).await;
        resps
            .into_iter()
            .filter_map(|resp| resp)
            .collect::<Vec<(u64, RegionEpoch)>>()
    }

    async fn region_resolved_ts_store(
        regions: Vec<u64>,
        store_meta: Arc<Mutex<StoreMeta>>,
        pd_client: Arc<dyn PdClient>,
        security_mgr: Arc<SecurityManager>,
        env: Arc<Environment>,
        cdc_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
        min_ts: TimeStamp,
    ) -> Vec<(u64, RegionEpoch)> {
        let region_has_quorum = |region: &Region, stores: Vec<u64>| {
            let mut voters = 0;
            let mut incoming_voters = 0;
            let mut demoting_voters = 0;

            let mut resp_voters = 0;
            let mut resp_incoming_voters = 0;
            let mut resp_demoting_voters = 0;

            region.get_peers().iter().for_each(|peer| {
                let mut in_resp = false;
                for store_id in &stores {
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
        };

        let find_store_id = |region: &Region, peer_id| {
            for peer in region.get_peers() {
                if peer.id == peer_id {
                    return Some(peer.store_id);
                }
            }
            None
        };

        // store_id -> leaders info, record the request to each stores
        let mut store_map: HashMap<u64, Vec<LeaderInfo>> = HashMap::default();
        // region_id -> region, cache the information of regions
        let mut region_map: HashMap<u64, Region> = HashMap::default();
        // region_id -> peers id, record the responses
        let mut resp_map: HashMap<u64, Vec<u64>> = HashMap::default();
        // region_id -> region epoch
        let mut epochs = HashMap::default();
        {
            let meta = store_meta.lock().unwrap();
            let store_id = match meta.store_id {
                Some(id) => id,
                None => return vec![],
            };
            for region_id in regions.clone() {
                if let Some(region) = meta.regions.get(&region_id) {
                    epochs.insert(region_id, region.get_region_epoch().clone());
                    if let Some((term, leader_id)) = meta.leaders.get(&region_id) {
                        match find_store_id(&region, *leader_id) {
                            None => continue,
                            Some(id) => {
                                if id != meta.store_id.unwrap() {
                                    continue;
                                }
                            }
                        }
                        for peer in region.get_peers() {
                            if peer.store_id == store_id && peer.id == *leader_id {
                                resp_map.entry(region_id).or_default().push(store_id);
                                continue;
                            }
                            if peer.get_role() == PeerRole::Learner {
                                continue;
                            }
                            let mut read_state = ReadState::default();
                            if let Some(peer_properties) = meta.peer_properties.get(&region_id) {
                                let pp = peer_properties
                                    .get::<RegionSafeTSTracker>()
                                    .expect("no peer property found");
                                let rrp = pp.downcast_ref::<RegionReadProgress>().unwrap();
                                read_state.set_applied_index(rrp.applied_index());
                                read_state.set_safe_ts(rrp.safe_ts());
                            }
                            let mut leader_info = LeaderInfo::default();
                            leader_info.set_peer_id(*leader_id);
                            leader_info.set_term(*term);
                            leader_info.set_region_id(region_id);
                            leader_info.set_region_epoch(region.get_region_epoch().clone());
                            leader_info.set_read_state(read_state);
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
                let client = box_try!(
                    get_tikv_client(store_id, pd_client, security_mgr, env, cdc_clients).await
                );
                let mut req = CheckLeaderRequest::default();
                req.set_regions(regions.into());
                req.set_ts(min_ts.into_inner());
                let res = box_try!(client.check_leader_async(&req)).await;
                let resp = box_try!(res);
                Result::Ok((store_id, resp))
            }
        });
        let resps = futures::future::join_all(stores).await;
        resps
            .into_iter()
            .filter_map(|res| {
                if res.is_err() {
                    debug!("region_resolved_ts_store meet error"; "res" => ?res);
                }
                Result::ok(res)
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
                if region_has_quorum(&region_map[&region_id], stores) {
                    Some((region_id, epochs.remove(&region_id).unwrap()))
                } else {
                    None
                }
            })
            .collect()
    }
}

async fn get_tikv_client(
    store_id: u64,
    pd_client: Arc<dyn PdClient>,
    security_mgr: Arc<SecurityManager>,
    env: Arc<Environment>,
    cdc_clients: Arc<Mutex<HashMap<u64, TikvClient>>>,
) -> Result<TikvClient> {
    if let Some(cli) = cdc_clients.lock().unwrap().get(&store_id) {
        return Ok(cli.clone());
    }
    let store = box_try!(pd_client.get_store_async(store_id).await);
    let channel = security_mgr.connect(ChannelBuilder::new(env), &store.address);
    let cli = TikvClient::new(channel);
    cdc_clients.lock().unwrap().insert(store_id, cli.clone());
    Ok(cli)
}
