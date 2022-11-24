// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use causal_ts::CausalTsProviderImpl;
use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use kvproto::{metapb, pdpb};
use pd_client::PdClient;
use raftstore::store::{util::KeysInfoFormatter, TxnExt};
use slog::{error, info, Logger};
use tikv_util::{time::UnixSecs, worker::Runnable};
use yatp::{task::future::TaskCell, Remote};

use crate::{batch::StoreRouter, router::PeerMsg};

mod region_heartbeat;
mod split;
mod store_heartbeat;
mod update_max_timestamp;

pub use region_heartbeat::RegionHeartbeatTask;

pub enum Task {
    RegionHeartbeat(RegionHeartbeatTask),
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        // TODO: StoreReport, StoreDrAutoSyncStatus
    },
    DestroyPeer {
        region_id: u64,
    },
    AskBatchSplit {
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
    },
    ReportBatchSplit {
        regions: Vec<metapb::Region>,
    },
    UpdateMaxTimestamp {
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::RegionHeartbeat(ref hb_task) => write!(
                f,
                "region heartbeat for region {:?}, leader {}",
                hb_task.region,
                hb_task.peer.get_id(),
            ),
            Task::StoreHeartbeat { ref stats, .. } => {
                write!(f, "store heartbeat stats: {:?}", stats)
            }
            Task::DestroyPeer { ref region_id } => {
                write!(f, "destroy peer of region {}", region_id)
            }
            Task::AskBatchSplit {
                ref region,
                ref split_keys,
                ..
            } => write!(
                f,
                "ask split region {} with {}",
                region.get_id(),
                KeysInfoFormatter(split_keys.iter())
            ),
            Task::ReportBatchSplit { ref regions } => write!(f, "report split {:?}", regions),
            Task::UpdateMaxTimestamp { region_id, .. } => write!(
                f,
                "update the max timestamp for region {} in the concurrency manager",
                region_id
            ),
        }
    }
}

pub struct Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    store_id: u64,
    pd_client: Arc<T>,
    raft_engine: ER,
    tablet_factory: Arc<dyn TabletFactory<EK>>,
    router: StoreRouter<EK, ER>,

    remote: Remote<TaskCell>,

    region_peers: HashMap<u64, region_heartbeat::PeerStat>,

    // For store_heartbeat.
    start_ts: UnixSecs,
    store_stat: store_heartbeat::StoreStat,

    // For region_heartbeat.
    region_cpu_records: HashMap<u64, u32>,
    is_hb_receiver_scheduled: bool,

    // For update_max_timestamp.
    concurrency_manager: ConcurrencyManager,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,

    logger: Logger,
    shutdown: Arc<AtomicBool>,
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn new(
        store_id: u64,
        pd_client: Arc<T>,
        raft_engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        router: StoreRouter<EK, ER>,
        remote: Remote<TaskCell>,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        logger: Logger,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store_id,
            pd_client,
            raft_engine,
            tablet_factory,
            router,
            remote,
            region_peers: HashMap::default(),
            start_ts: UnixSecs::zero(),
            store_stat: store_heartbeat::StoreStat::default(),
            region_cpu_records: HashMap::default(),
            is_hb_receiver_scheduled: false,
            concurrency_manager,
            causal_ts_provider,
            logger,
            shutdown,
        }
    }
}

impl<EK, ER, T> Runnable for Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        self.maybe_schedule_heartbeat_receiver();
        match task {
            Task::RegionHeartbeat(task) => self.handle_region_heartbeat(task),
            Task::StoreHeartbeat { stats } => self.handle_store_heartbeat(stats),
            Task::DestroyPeer { region_id } => self.handle_destroy_peer(region_id),
            Task::AskBatchSplit {
                region,
                split_keys,
                peer,
                right_derive,
            } => self.handle_ask_batch_split(region, split_keys, peer, right_derive),
            Task::ReportBatchSplit { regions } => self.handle_report_batch_split(regions),
            Task::UpdateMaxTimestamp {
                region_id,
                initial_status,
                txn_ext,
            } => self.handle_update_max_timestamp(region_id, initial_status, txn_ext),
        }
    }
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    fn handle_destroy_peer(&mut self, region_id: u64) {
        match self.region_peers.remove(&region_id) {
            None => {}
            Some(_) => {
                info!(self.logger, "remove peer statistic record in pd"; "region_id" => region_id)
            }
        }
    }
}

pub mod requests {
    use kvproto::raft_cmdpb::{
        AdminCmdType, AdminRequest, ChangePeerRequest, ChangePeerV2Request, RaftCmdRequest,
        SplitRequest,
    };
    use raft::eraftpb::ConfChangeType;

    use super::*;

    pub fn send_admin_request<EK, ER>(
        logger: &Logger,
        router: &StoreRouter<EK, ER>,
        region_id: u64,
        epoch: metapb::RegionEpoch,
        peer: metapb::Peer,
        request: AdminRequest,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let cmd_type = request.get_cmd_type();

        let mut req = RaftCmdRequest::default();
        req.mut_header().set_region_id(region_id);
        req.mut_header().set_region_epoch(epoch);
        req.mut_header().set_peer(peer);
        req.set_admin_request(request);

        let (msg, _) = PeerMsg::raft_command(req);
        if let Err(e) = router.send(region_id, msg) {
            error!(
                logger,
                "send request failed";
                "region_id" => region_id, "cmd_type" => ?cmd_type, "err" => ?e,
            );
        }
    }

    pub fn new_change_peer_request(
        change_type: ConfChangeType,
        peer: metapb::Peer,
    ) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::ChangePeer);
        req.mut_change_peer().set_change_type(change_type);
        req.mut_change_peer().set_peer(peer);
        req
    }

    pub fn new_change_peer_v2_request(changes: Vec<pdpb::ChangePeer>) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::ChangePeerV2);
        let change_peer_reqs = changes
            .into_iter()
            .map(|mut c| {
                let mut cp = ChangePeerRequest::default();
                cp.set_change_type(c.get_change_type());
                cp.set_peer(c.take_peer());
                cp
            })
            .collect();
        let mut cp = ChangePeerV2Request::default();
        cp.set_changes(change_peer_reqs);
        req.set_change_peer_v2(cp);
        req
    }

    pub fn new_split_region_request(
        split_key: Vec<u8>,
        new_region_id: u64,
        peer_ids: Vec<u64>,
        right_derive: bool,
    ) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::Split);
        req.mut_split().set_split_key(split_key);
        req.mut_split().set_new_region_id(new_region_id);
        req.mut_split().set_new_peer_ids(peer_ids);
        req.mut_split().set_right_derive(right_derive);
        req
    }

    pub fn new_batch_split_region_request(
        split_keys: Vec<Vec<u8>>,
        ids: Vec<pdpb::SplitId>,
        right_derive: bool,
    ) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::BatchSplit);
        req.mut_splits().set_right_derive(right_derive);
        let mut requests = Vec::with_capacity(ids.len());
        for (mut id, key) in ids.into_iter().zip(split_keys) {
            let mut split = SplitRequest::default();
            split.set_split_key(key);
            split.set_new_region_id(id.get_new_region_id());
            split.set_new_peer_ids(id.take_new_peer_ids());
            requests.push(split);
        }
        req.mut_splits().set_requests(requests.into());
        req
    }

    pub fn new_transfer_leader_request(
        peer: metapb::Peer,
        peers: Vec<metapb::Peer>,
    ) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::TransferLeader);
        req.mut_transfer_leader().set_peer(peer);
        req.mut_transfer_leader().set_peers(peers.into());
        req
    }

    pub fn new_merge_request(merge: pdpb::Merge) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::PrepareMerge);
        req.mut_prepare_merge()
            .set_target(merge.get_target().to_owned());
        req
    }
}
