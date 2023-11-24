// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    sync::{atomic::AtomicBool, Arc},
};

use causal_ts::CausalTsProviderImpl;
use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry};
use kvproto::{metapb, pdpb};
use pd_client::{BucketStat, PdClient};
use raftstore::store::{
<<<<<<< HEAD
    util::KeysInfoFormatter, AutoSplitController, Config, FlowStatsReporter, PdStatsMonitor,
    ReadStats, RegionReadProgressRegistry, SplitInfo, StoreStatsReporter, TabletSnapManager,
    TxnExt, WriteStats, NUM_COLLECT_STORE_INFOS_PER_HEARTBEAT,
=======
    metrics::STORE_INSPECT_DURATION_HISTOGRAM,
    util::{KeysInfoFormatter, LatencyInspector, RaftstoreDuration},
    AutoSplitController, Config, FlowStatsReporter, PdStatsMonitor, ReadStats, SplitInfo,
    StoreStatsReporter, TabletSnapManager, TxnExt, WriteStats,
    NUM_COLLECT_STORE_INFOS_PER_HEARTBEAT,
>>>>>>> bc1ae30437 (pd_client: support dynamically modifying `min-resolved-ts` report interval and reduce retry times (#15837))
};
use resource_metering::{Collector, CollectorRegHandle, RawRecords};
use slog::{error, Logger};
use tikv_util::{
    config::VersionTrack,
    time::UnixSecs,
    worker::{Runnable, Scheduler},
};
use yatp::{task::future::TaskCell, Remote};

use crate::{
    batch::StoreRouter,
    router::{CmdResChannel, PeerMsg},
};

mod misc;
mod region;
mod split;
mod store;

pub use region::RegionHeartbeatTask;

type RecordPairVec = Vec<pdpb::RecordPair>;

pub enum Task {
    // In store.rs.
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        // TODO: StoreReport, StoreDrAutoSyncStatus
    },
    UpdateStoreInfos {
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    },
    // In region.rs.
    RegionHeartbeat(RegionHeartbeatTask),
    ReportRegionBuckets(BucketStat),
    UpdateReadStats(ReadStats),
    UpdateWriteStats(WriteStats),
    UpdateRegionCpuRecords(Arc<RawRecords>),
    DestroyPeer {
        region_id: u64,
    },
    // In split.rs.
    AskBatchSplit {
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
        share_source_region_size: bool,
        ch: CmdResChannel,
    },
    ReportBatchSplit {
        regions: Vec<metapb::Region>,
    },
    AutoSplit {
        split_infos: Vec<SplitInfo>,
    },
    // In misc.rs.
    UpdateMaxTimestamp {
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    },
    ReportBuckets(BucketStat),
    ReportMinResolvedTs {
        store_id: u64,
        min_resolved_ts: u64,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::StoreHeartbeat { ref stats, .. } => {
                write!(f, "store heartbeat stats: {stats:?}")
            }
            Task::UpdateStoreInfos {
                ref cpu_usages,
                ref read_io_rates,
                ref write_io_rates,
            } => write!(
                f,
                "get store's information: cpu_usages {:?}, read_io_rates {:?}, write_io_rates {:?}",
                cpu_usages, read_io_rates, write_io_rates,
            ),
            Task::RegionHeartbeat(ref hb_task) => write!(
                f,
                "region heartbeat for region {:?}, leader {}",
                hb_task.region,
                hb_task.peer.get_id(),
            ),
            Task::ReportRegionBuckets(ref buckets) => write!(f, "report buckets: {:?}", buckets),
            Task::UpdateReadStats(ref stats) => {
                write!(f, "update read stats: {stats:?}")
            }
            Task::UpdateWriteStats(ref stats) => {
                write!(f, "update write stats: {stats:?}")
            }
            Task::UpdateRegionCpuRecords(ref cpu_records) => {
                write!(f, "get region cpu records: {:?}", cpu_records)
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
            Task::AutoSplit { ref split_infos } => {
                write!(f, "auto split split regions, num is {}", split_infos.len())
            }
            Task::UpdateMaxTimestamp { region_id, .. } => write!(
                f,
                "update the max timestamp for region {} in the concurrency manager",
                region_id
            ),
            Task::ReportBuckets(ref buckets) => write!(f, "report buckets: {:?}", buckets),
            Task::ReportMinResolvedTs {
                store_id,
                min_resolved_ts,
            } => write!(
                f,
                "report min resolved ts: store {}, resolved ts {}",
                store_id, min_resolved_ts,
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
    tablet_registry: TabletRegistry<EK>,
    snap_mgr: TabletSnapManager,
    router: StoreRouter<EK, ER>,
    stats_monitor: PdStatsMonitor<PdReporter>,

    remote: Remote<TaskCell>,

    // For store.
    start_ts: UnixSecs,
    store_stat: store::StoreStat,

    // For region.
    region_peers: HashMap<u64, region::PeerStat>,
    region_buckets: HashMap<u64, region::ReportBucket>,
    // region_id -> total_cpu_time_ms (since last region heartbeat)
    region_cpu_records: HashMap<u64, u32>,
    is_hb_receiver_scheduled: bool,

    // For update_max_timestamp.
    concurrency_manager: ConcurrencyManager,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,

    logger: Logger,
    shutdown: Arc<AtomicBool>,
    cfg: Arc<VersionTrack<Config>>,
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
        tablet_registry: TabletRegistry<EK>,
        snap_mgr: TabletSnapManager,
        router: StoreRouter<EK, ER>,
        remote: Remote<TaskCell>,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        pd_scheduler: Scheduler<Task>,
        auto_split_controller: AutoSplitController,
        collector_reg_handle: CollectorRegHandle,
        logger: Logger,
        shutdown: Arc<AtomicBool>,
        cfg: Arc<VersionTrack<Config>>,
    ) -> Result<Self, std::io::Error> {
        let mut stats_monitor = PdStatsMonitor::new(
<<<<<<< HEAD
            cfg.value().pd_store_heartbeat_tick_interval.0 / NUM_COLLECT_STORE_INFOS_PER_HEARTBEAT,
            cfg.value().report_min_resolved_ts_interval.0,
            PdReporter::new(pd_scheduler, logger.clone()),
        );
        stats_monitor.start(
            auto_split_controller,
            region_read_progress,
            collector_reg_handle,
            store_id,
        )?;
=======
            store_heartbeat_interval / NUM_COLLECT_STORE_INFOS_PER_HEARTBEAT,
            cfg.value().inspect_interval.0,
            PdReporter::new(pd_scheduler, logger.clone()),
        );
        stats_monitor.start(auto_split_controller, collector_reg_handle)?;
        let slowness_stats = slowness::SlownessStatistics::new(&cfg.value());
>>>>>>> bc1ae30437 (pd_client: support dynamically modifying `min-resolved-ts` report interval and reduce retry times (#15837))
        Ok(Self {
            store_id,
            pd_client,
            raft_engine,
            tablet_registry,
            snap_mgr,
            router,
            stats_monitor,
            remote,
            start_ts: UnixSecs::zero(),
            store_stat: store::StoreStat::default(),
            region_peers: HashMap::default(),
            region_buckets: HashMap::default(),
            region_cpu_records: HashMap::default(),
            is_hb_receiver_scheduled: false,
            concurrency_manager,
            causal_ts_provider,
            logger,
            shutdown,
            cfg,
        })
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
            Task::StoreHeartbeat { stats } => self.handle_store_heartbeat(stats),
            Task::UpdateStoreInfos {
                cpu_usages,
                read_io_rates,
                write_io_rates,
            } => self.handle_update_store_infos(cpu_usages, read_io_rates, write_io_rates),
            Task::RegionHeartbeat(task) => self.handle_region_heartbeat(task),
            Task::ReportRegionBuckets(buckets) => self.handle_report_region_buckets(buckets),
            Task::UpdateReadStats(stats) => self.handle_update_read_stats(stats),
            Task::UpdateWriteStats(stats) => self.handle_update_write_stats(stats),
            Task::UpdateRegionCpuRecords(records) => self.handle_update_region_cpu_records(records),
            Task::DestroyPeer { region_id } => self.handle_destroy_peer(region_id),
            Task::AskBatchSplit {
                region,
                split_keys,
                peer,
                right_derive,
                ch,
                share_source_region_size,
            } => self.handle_ask_batch_split(
                region,
                split_keys,
                peer,
                right_derive,
                share_source_region_size,
                ch,
            ),
            Task::ReportBatchSplit { regions } => self.handle_report_batch_split(regions),
            Task::AutoSplit { split_infos } => self.handle_auto_split(split_infos),
            Task::UpdateMaxTimestamp {
                region_id,
                initial_status,
                txn_ext,
            } => self.handle_update_max_timestamp(region_id, initial_status, txn_ext),
            Task::ReportBuckets(buckets) => self.handle_report_region_buckets(buckets),
            Task::ReportMinResolvedTs {
                store_id,
                min_resolved_ts,
            } => self.handle_report_min_resolved_ts(store_id, min_resolved_ts),
        }
    }
}

#[derive(Clone)]
pub struct PdReporter {
    scheduler: Scheduler<Task>,
    logger: Logger,
}

impl PdReporter {
    pub fn new(scheduler: Scheduler<Task>, logger: Logger) -> Self {
        PdReporter { scheduler, logger }
    }
}

impl FlowStatsReporter for PdReporter {
    fn report_read_stats(&self, stats: ReadStats) {
        if let Err(e) = self.scheduler.schedule(Task::UpdateReadStats(stats)) {
            error!(self.logger, "Failed to send read flow statistics"; "err" => ?e);
        }
    }

    fn report_write_stats(&self, stats: WriteStats) {
        if let Err(e) = self.scheduler.schedule(Task::UpdateWriteStats(stats)) {
            error!(self.logger, "Failed to send write flow statistics"; "err" => ?e);
        }
    }
}

impl Collector for PdReporter {
    fn collect(&self, records: Arc<RawRecords>) {
        self.scheduler
            .schedule(Task::UpdateRegionCpuRecords(records))
            .ok();
    }
}

impl StoreStatsReporter for PdReporter {
    fn report_store_infos(
        &self,
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    ) {
        let task = Task::UpdateStoreInfos {
            cpu_usages,
            read_io_rates,
            write_io_rates,
        };
        if let Err(e) = self.scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to send store infos to pd worker";
                "err" => ?e,
            );
        }
    }

    fn report_min_resolved_ts(&self, store_id: u64, min_resolved_ts: u64) {
        let task = Task::ReportMinResolvedTs {
            store_id,
            min_resolved_ts,
        };
        if let Err(e) = self.scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to send min resolved ts to pd worker";
                "err" => ?e,
            );
        }
    }

    fn auto_split(&self, split_infos: Vec<SplitInfo>) {
        let task = Task::AutoSplit { split_infos };
        if let Err(e) = self.scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to send split infos to pd worker";
                "err" => ?e,
            );
        }
    }
}

mod requests {
    use kvproto::raft_cmdpb::{
        AdminCmdType, AdminRequest, ChangePeerRequest, ChangePeerV2Request, RaftCmdRequest,
    };
    use raft::eraftpb::ConfChangeType;

    use super::*;
    use crate::router::RaftRequest;

    pub fn send_admin_request<EK, ER>(
        logger: &Logger,
        router: &StoreRouter<EK, ER>,
        region_id: u64,
        epoch: metapb::RegionEpoch,
        peer: metapb::Peer,
        request: AdminRequest,
        ch: Option<CmdResChannel>,
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

        let msg = match ch {
            Some(ch) => PeerMsg::AdminCommand(RaftRequest::new(req, ch)),
            None => PeerMsg::admin_command(req).0,
        };
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
