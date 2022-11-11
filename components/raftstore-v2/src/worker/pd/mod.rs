// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use kvproto::{
    metapb, pdpb,
    raft_cmdpb::{AdminRequest, RaftCmdRequest},
};
use pd_client::PdClient;
use raftstore::store::util::KeysInfoFormatter;
use slog::{error, info, Logger};
use tikv_util::{time::UnixSecs, worker::Runnable};
use yatp::{task::future::TaskCell, Remote};

mod heartbeat;
mod split;
mod store_heartbeat;

pub use heartbeat::Task as HeartbeatTask;

use crate::{batch::StoreRouter, router::PeerMsg};

pub enum Task {
    Heartbeat(HeartbeatTask),
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
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Heartbeat(ref hb_task) => write!(
                f,
                "heartbeat for region {:?}, leader {}",
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
        }
    }
}

pub struct Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pd_client: Arc<T>,
    raft_engine: ER,
    tablet_factory: Arc<dyn TabletFactory<EK>>,
    router: StoreRouter<EK, ER>,

    remote: Remote<TaskCell>,

    region_peers: HashMap<u64, heartbeat::PeerStat>,

    start_ts: UnixSecs,
    store_stat: store_heartbeat::StoreStat,

    region_cpu_records: HashMap<u64, u32>,

    logger: Logger,
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn new(
        pd_client: Arc<T>,
        raft_engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        router: StoreRouter<EK, ER>,
        remote: Remote<TaskCell>,
        logger: Logger,
    ) -> Self {
        Self {
            pd_client,
            raft_engine,
            tablet_factory,
            router,
            remote,
            region_peers: HashMap::default(),
            start_ts: UnixSecs::zero(),
            store_stat: store_heartbeat::StoreStat::default(),
            region_cpu_records: HashMap::default(),
            logger,
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
        match task {
            Task::Heartbeat(task) => self.handle_heartbeat(task),
            Task::StoreHeartbeat { stats } => self.handle_store_heartbeat(stats),
            Task::DestroyPeer { region_id } => self.handle_destroy_peer(region_id),
            Task::AskBatchSplit {
                region,
                split_keys,
                peer,
                right_derive,
            } => self.handle_ask_batch_split(region, split_keys, peer, right_derive),
            Task::ReportBatchSplit { regions } => self.handle_report_batch_split(regions),
            _ => unimplemented!(),
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
