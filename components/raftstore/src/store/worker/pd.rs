// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    cmp::Ordering as CmpOrdering,
    fmt::{self, Display, Formatter},
    io, mem,
    sync::{
        Arc, Mutex, RwLock,
        atomic::Ordering,
        mpsc::{self, Receiver, Sender, SyncSender},
    },
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use causal_ts::{CausalTsProvider, CausalTsProviderImpl};
use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use futures::{FutureExt, compat::Future01CompatExt};
use health_controller::{
    HealthController,
    reporters::{RaftstoreReporter, RaftstoreReporterConfig},
    types::{InspectFactor, LatencyInspector, RaftstoreDuration},
};
use kvproto::{
    kvrpcpb::DiskFullOpt,
    metapb, pdpb,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, BatchSwitchWitnessRequest, ChangePeerRequest,
        ChangePeerV2Request, RaftCmdRequest, SplitRequest, SwitchWitnessRequest,
    },
    raft_serverpb::RaftMessage,
    replication_modepb::{RegionReplicationStatus, StoreDrAutoSyncStatus},
};
use pd_client::{BucketStat, Error, PdClient, RegionStat, RegionWriteCfCopDetail, metrics::*};
use prometheus::local::LocalHistogram;
use raft::eraftpb::ConfChangeType;
use resource_metering::{Collector, CollectorGuard, CollectorRegHandle, RawRecords};
use service::service_manager::GrpcServiceManager;
use tikv_util::{
    GLOBAL_SERVER_READINESS, box_err, debug, error, info,
    metrics::ThreadInfoStatistics,
    store::QueryStats,
    sys::{SysQuota, disk, thread::StdThreadBuildWrapper},
    thd_name,
    time::{Instant as TiInstant, UnixSecs},
    timer::GLOBAL_TIMER_HANDLE,
    topn::TopN,
    warn,
    worker::{Runnable, ScheduleError, Scheduler},
};
use yatp::Remote;

use super::split_controller::AutoSplitControllerContext;
use crate::{
    coprocessor::CoprocessorHost,
    router::RaftStoreRouter,
    store::{
        Callback, CasualMessage, Config, PeerMsg, RaftCmdExtraOpts, RaftCommand, RaftRouter,
        SnapManager, StoreMsg, TxnExt,
        cmd_resp::new_error,
        metrics::*,
        unsafe_recovery::{
            UnsafeRecoveryExecutePlanSyncer, UnsafeRecoveryForceLeaderSyncer, UnsafeRecoveryHandle,
        },
        util::{KeysInfoFormatter, is_epoch_stale},
        worker::{
            AutoSplitController, ReadStats, SplitConfigChange, WriteStats,
            split_controller::{SplitInfo, TOP_N},
        },
    },
};

pub const NUM_COLLECT_STORE_INFOS_PER_HEARTBEAT: u32 = 2;
/// The upper bound of buffered stats messages.
/// It prevents unexpected memory buildup when AutoSplitController
/// runs slowly.
const STATS_CHANNEL_CAPACITY_LIMIT: usize = 128;

type RecordPairVec = Vec<pdpb::RecordPair>;

#[derive(Default, Debug, Clone)]
pub struct FlowStatistics {
    pub read_keys: usize,
    pub read_bytes: usize,
}

impl FlowStatistics {
    pub fn add(&mut self, other: &Self) {
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
        self.read_keys = self.read_keys.saturating_add(other.read_keys);
    }
}

// Reports flow statistics to outside.
pub trait FlowStatsReporter: Send + Clone + Sync + 'static {
    // TODO: maybe we need to return a Result later?
    fn report_read_stats(&self, read_stats: ReadStats);

    fn report_write_stats(&self, write_stats: WriteStats);
}

impl<EK> FlowStatsReporter for Scheduler<Task<EK>>
where
    EK: KvEngine,
{
    fn report_read_stats(&self, read_stats: ReadStats) {
        if let Err(e) = self.schedule(Task::ReadStats { read_stats }) {
            warn!("Failed to send read flow statistics"; "err" => ?e);
        }
    }

    fn report_write_stats(&self, write_stats: WriteStats) {
        if let Err(e) = self.schedule(Task::WriteStats { write_stats }) {
            warn!("Failed to send write flow statistics"; "err" => ?e);
        }
    }
}

pub struct HeartbeatTask {
    pub term: u64,
    pub region: metapb::Region,
    pub peer: metapb::Peer,
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub approximate_size: Option<u64>,
    pub approximate_keys: Option<u64>,
    pub replication_status: Option<RegionReplicationStatus>,
    pub wait_data_peers: Vec<u64>,
}

/// Uses an asynchronous thread to tell PD something.
pub enum Task<EK>
where
    EK: KvEngine,
{
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        share_source_region_size: bool,
        callback: Callback<EK::Snapshot>,
    },
    AskBatchSplit {
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        share_source_region_size: bool,
        callback: Callback<EK::Snapshot>,
    },
    AutoSplit {
        split_infos: Vec<SplitInfo>,
    },
    Heartbeat(HeartbeatTask),
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        report: Option<pdpb::StoreReport>,
        dr_autosync_status: Option<StoreDrAutoSyncStatus>,
    },
    ReportBatchSplit {
        regions: Vec<metapb::Region>,
    },
    ValidatePeer {
        region: metapb::Region,
        peer: metapb::Peer,
    },
    ReadStats {
        read_stats: ReadStats,
    },
    WriteStats {
        write_stats: WriteStats,
    },
    DestroyPeer {
        region_id: u64,
    },
    StoreInfos {
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    },
    UpdateMaxTimestamp {
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    },
    QueryRegionLeader {
        region_id: u64,
    },
    UpdateSlowScore {
        id: u64,
        factor: InspectFactor,
        duration: RaftstoreDuration,
    },
    RegionCpuRecords(Arc<RawRecords>),
    ReportMinResolvedTs {
        store_id: u64,
        min_resolved_ts: u64,
    },
    ReportBuckets(BucketStat),
    ControlGrpcServer(pdpb::ControlGrpcEvent),
    InspectLatency {
        factor: InspectFactor,
    },
}

pub struct StoreStat {
    pub engine_total_bytes_read: u64,
    pub engine_total_keys_read: u64,
    pub engine_total_query_num: QueryStats,
    pub engine_last_total_bytes_read: u64,
    pub engine_last_total_keys_read: u64,
    pub engine_last_query_num: QueryStats,
    pub engine_last_capacity_size: u64,
    pub engine_last_used_size: u64,
    pub engine_last_available_size: u64,
    pub last_report_ts: UnixSecs,

    pub region_bytes_read: LocalHistogram,
    pub region_keys_read: LocalHistogram,
    pub region_bytes_written: LocalHistogram,
    pub region_keys_written: LocalHistogram,

    pub store_cpu_usages: RecordPairVec,
    pub store_read_io_rates: RecordPairVec,
    pub store_write_io_rates: RecordPairVec,

    store_cpu_quota: f64, // quota of cpu usage
    store_cpu_busy_thd: f64,
}

impl Default for StoreStat {
    fn default() -> StoreStat {
        StoreStat {
            region_bytes_read: REGION_READ_BYTES_HISTOGRAM.local(),
            region_keys_read: REGION_READ_KEYS_HISTOGRAM.local(),
            region_bytes_written: REGION_WRITTEN_BYTES_HISTOGRAM.local(),
            region_keys_written: REGION_WRITTEN_KEYS_HISTOGRAM.local(),

            last_report_ts: UnixSecs::zero(),
            engine_total_bytes_read: 0,
            engine_total_keys_read: 0,
            engine_last_total_bytes_read: 0,
            engine_last_total_keys_read: 0,
            engine_last_capacity_size: 0,
            engine_last_used_size: 0,
            engine_last_available_size: 0,
            engine_total_query_num: QueryStats::default(),
            engine_last_query_num: QueryStats::default(),

            store_cpu_usages: RecordPairVec::default(),
            store_read_io_rates: RecordPairVec::default(),
            store_write_io_rates: RecordPairVec::default(),

            store_cpu_quota: 0.0_f64,
            store_cpu_busy_thd: 0.8_f64,
        }
    }
}

impl StoreStat {
    fn set_cpu_quota(&mut self, cpu_cores: f64, busy_thd: f64) {
        self.store_cpu_quota = cpu_cores * 100.0;
        self.store_cpu_busy_thd = busy_thd;
    }

    fn maybe_busy(&self) -> bool {
        if self.store_cpu_quota < 1.0 || self.store_cpu_busy_thd > 1.0 {
            return false;
        }

        let mut cpu_usage = 0_u64;
        for record in self.store_cpu_usages.iter() {
            cpu_usage += record.get_value();
        }

        (cpu_usage as f64 / self.store_cpu_quota) >= self.store_cpu_busy_thd
    }
}

#[derive(Default)]
pub struct PeerStat {
    pub read_bytes: u64,
    pub read_keys: u64,
    pub query_stats: QueryStats,
    pub cop_detail: RegionWriteCfCopDetail,
    // last_region_report_attributes records the state of the last region heartbeat
    pub last_region_report_read_bytes: u64,
    pub last_region_report_read_keys: u64,
    pub last_region_report_query_stats: QueryStats,
    pub last_region_report_cop_detail: RegionWriteCfCopDetail,
    pub last_region_report_written_bytes: u64,
    pub last_region_report_written_keys: u64,
    pub last_region_report_ts: UnixSecs,
    // last_store_report_attributes records the state of the last store heartbeat
    pub last_store_report_read_bytes: u64,
    pub last_store_report_read_keys: u64,
    pub last_store_report_query_stats: QueryStats,
    pub approximate_keys: u64,
    pub approximate_size: u64,
}

#[derive(Default)]
struct ReportBucket {
    current_stat: BucketStat,
    last_report_stat: Option<BucketStat>,
    last_report_ts: UnixSecs,
}

impl ReportBucket {
    fn new(current_stat: BucketStat) -> Self {
        Self {
            current_stat,
            ..Default::default()
        }
    }

    fn new_report(&mut self, report_ts: UnixSecs) -> BucketStat {
        self.last_report_ts = report_ts;
        match self.last_report_stat.replace(self.current_stat.clone()) {
            Some(last) => {
                let mut delta = BucketStat::from_meta(self.current_stat.meta.clone());
                // Buckets may be changed, recalculate last stats according to current meta.
                delta.merge(&last);
                for i in 0..delta.meta.keys.len() - 1 {
                    delta.stats.write_bytes[i] =
                        self.current_stat.stats.write_bytes[i] - delta.stats.write_bytes[i];
                    delta.stats.write_keys[i] =
                        self.current_stat.stats.write_keys[i] - delta.stats.write_keys[i];
                    delta.stats.write_qps[i] =
                        self.current_stat.stats.write_qps[i] - delta.stats.write_qps[i];

                    delta.stats.read_bytes[i] =
                        self.current_stat.stats.read_bytes[i] - delta.stats.read_bytes[i];
                    delta.stats.read_keys[i] =
                        self.current_stat.stats.read_keys[i] - delta.stats.read_keys[i];
                    delta.stats.read_qps[i] =
                        self.current_stat.stats.read_qps[i] - delta.stats.read_qps[i];
                }
                delta
            }
            None => self.current_stat.clone(),
        }
    }
}

#[derive(Default, Clone)]
struct PeerCmpReadStat {
    pub region_id: u64,
    pub report_stat: u64,
}

impl Ord for PeerCmpReadStat {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.report_stat.cmp(&other.report_stat)
    }
}

impl Eq for PeerCmpReadStat {}

impl PartialEq for PeerCmpReadStat {
    fn eq(&self, other: &Self) -> bool {
        self.report_stat == other.report_stat
    }
}

impl PartialOrd for PeerCmpReadStat {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.report_stat.cmp(&other.report_stat))
    }
}

impl<EK> Display for Task<EK>
where
    EK: KvEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::AskSplit {
                ref region,
                ref split_key,
                ..
            } => write!(
                f,
                "ask split region {} with key {}",
                region.get_id(),
                log_wrappers::Value::key(split_key),
            ),
            Task::AutoSplit { ref split_infos } => {
                write!(f, "auto split split regions, num is {}", split_infos.len())
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
            Task::Heartbeat(ref hb_task) => write!(
                f,
                "heartbeat for region {:?}, leader {}, replication status {:?}",
                hb_task.region,
                hb_task.peer.get_id(),
                hb_task.replication_status
            ),
            Task::StoreHeartbeat { ref stats, .. } => {
                write!(f, "store heartbeat stats: {:?}", stats)
            }
            Task::ReportBatchSplit { ref regions } => write!(f, "report split {:?}", regions),
            Task::ValidatePeer {
                ref region,
                ref peer,
            } => write!(f, "validate peer {:?} with region {:?}", peer, region),
            Task::ReadStats { ref read_stats } => {
                write!(f, "get the read statistics {:?}", read_stats)
            }
            Task::WriteStats { ref write_stats } => {
                write!(f, "get the write statistics {:?}", write_stats)
            }
            Task::DestroyPeer { ref region_id } => {
                write!(f, "destroy peer of region {}", region_id)
            }
            Task::StoreInfos {
                ref cpu_usages,
                ref read_io_rates,
                ref write_io_rates,
            } => write!(
                f,
                "get store's information: cpu_usages {:?}, read_io_rates {:?}, write_io_rates {:?}",
                cpu_usages, read_io_rates, write_io_rates,
            ),
            Task::UpdateMaxTimestamp { region_id, .. } => write!(
                f,
                "update the max timestamp for region {} in the concurrency manager",
                region_id
            ),
            Task::QueryRegionLeader { region_id } => {
                write!(f, "query the leader of region {}", region_id)
            }
            Task::UpdateSlowScore {
                id,
                factor,
                ref duration,
            } => {
                write!(
                    f,
                    "compute slow score: id {}, factor: {:?}, duration {:?}",
                    id, factor, duration
                )
            }
            Task::RegionCpuRecords(ref cpu_records) => {
                write!(f, "get region cpu records: {:?}", cpu_records)
            }
            Task::ReportMinResolvedTs {
                store_id,
                min_resolved_ts,
            } => {
                write!(
                    f,
                    "report min resolved ts: store {}, resolved ts {}",
                    store_id, min_resolved_ts
                )
            }
            Task::ReportBuckets(ref buckets) => {
                write!(f, "report buckets: {:?}", buckets)
            }
            Task::ControlGrpcServer(ref event) => {
                write!(f, "control grpc server: {:?}", event)
            }
            Task::InspectLatency { factor } => {
                write!(f, "inspect raftstore latency: {:?}", factor)
            }
        }
    }
}

const DEFAULT_LOAD_BASE_SPLIT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_COLLECT_TICK_INTERVAL: Duration = Duration::from_secs(1);

fn default_collect_tick_interval() -> Duration {
    fail_point!("mock_collect_tick_interval", |_| {
        Duration::from_millis(1)
    });
    DEFAULT_COLLECT_TICK_INTERVAL
}

/// Determines the minimal interval for latency inspection ticks based on raft
/// and kvdb inspection intervals.
///
/// This function handles different scenarios for latency inspection:
/// 1. Both intervals are zero: Inspection is disabled, returns a large interval
///    (1 hour)
/// 2. Only raft interval is zero: Uses kvdb interval (raft inspection disabled)
/// 3. Only kvdb interval is zero: Uses raft interval (kvdb inspection disabled)
/// 4. Both intervals non-zero: Uses the smaller of the two intervals
///
/// # Arguments
///
/// * `inspect_latency_interval` - Interval for raft latency inspection
/// * `inspect_kvdb_latency_interval` - Interval for kvdb latency inspection
///
/// # Returns
///
/// The minimal interval that should be used for latency inspection ticks
fn get_minimal_inspect_tick_interval(
    inspect_latency_interval: Duration,
    inspect_kvdb_latency_interval: Duration,
) -> Duration {
    match (
        inspect_latency_interval.is_zero(),
        inspect_kvdb_latency_interval.is_zero(),
    ) {
        (true, true) => {
            // Both inspections are disabled - return a large interval to avoid misleading
            // tick checks
            Duration::from_secs(3600)
        }
        (true, false) => {
            // raft inspection disabled - use kvdb interval
            inspect_kvdb_latency_interval
        }
        (false, true) => {
            // kvdb inspection disabled - use raft interval
            inspect_latency_interval
        }
        (false, false) => {
            // Both inspections enabled - use the smaller interval
            std::cmp::min(inspect_latency_interval, inspect_kvdb_latency_interval)
        }
    }
}

#[inline]
fn convert_record_pairs(m: HashMap<String, u64>) -> RecordPairVec {
    m.into_iter()
        .map(|(k, v)| {
            let mut pair = pdpb::RecordPair::default();
            pair.set_key(k);
            pair.set_value(v);
            pair
        })
        .collect()
}

#[derive(Clone)]
pub struct WrappedScheduler<EK: KvEngine>(Scheduler<Task<EK>>);

impl<EK> Collector for WrappedScheduler<EK>
where
    EK: KvEngine,
{
    fn collect(&self, records: Arc<RawRecords>) {
        self.0.schedule(Task::RegionCpuRecords(records)).ok();
    }
}

pub trait StoreStatsReporter: Send + Clone + Sync + 'static + Collector {
    fn report_store_infos(
        &self,
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    );
    fn report_min_resolved_ts(&self, store_id: u64, min_resolved_ts: u64);
    fn auto_split(&self, split_infos: Vec<SplitInfo>);
    fn update_latency_stats(&self, timer_tick: u64, factor: InspectFactor);
}

impl<EK> StoreStatsReporter for WrappedScheduler<EK>
where
    EK: KvEngine,
{
    fn report_store_infos(
        &self,
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    ) {
        let task = Task::StoreInfos {
            cpu_usages,
            read_io_rates,
            write_io_rates,
        };
        if let Err(e) = self.0.schedule(task) {
            error!(
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
        if let Err(e) = self.0.schedule(task) {
            warn!(
                "failed to send min resolved ts to pd worker";
                "err" => ?e,
            );
        }
    }

    fn auto_split(&self, split_infos: Vec<SplitInfo>) {
        let task = Task::AutoSplit { split_infos };
        if let Err(e) = self.0.schedule(task) {
            warn!(
                "failed to send split infos to pd worker";
                "err" => ?e,
            );
        }
    }

    fn update_latency_stats(&self, timer_tick: u64, factor: InspectFactor) {
        debug!("update latency statistics for raftstore-v1";
                "tick" => timer_tick);
        let task = Task::InspectLatency { factor };
        if let Err(e) = self.0.schedule(task) {
            warn!(
                "failed to send inspect raftstore latency task to pd worker";
                "err" => ?e,
            );
        }
    }
}

pub struct StatsMonitor<T>
where
    T: StoreStatsReporter,
{
    reporter: T,
    handle: Option<JoinHandle<()>>,
    timer: Option<Sender<bool>>,
    read_stats_sender: Option<SyncSender<ReadStats>>,
    cpu_stats_sender: Option<SyncSender<Arc<RawRecords>>>,
    collect_store_infos_interval: Duration,
    load_base_split_check_interval: Duration,
    collect_tick_interval: Duration,
    inspect_latency_interval: Duration,      // for raft mount path
    inspect_kvdb_latency_interval: Duration, // for kvdb mount path
    inspect_network_interval: Duration,
}

impl<T> StatsMonitor<T>
where
    T: StoreStatsReporter,
{
    pub fn new(
        interval: Duration,
        inspect_latency_interval: Duration,
        inspect_kvdb_latency_interval: Duration,
        inspect_network_interval: Duration,
        reporter: T,
    ) -> Self {
        StatsMonitor {
            reporter,
            handle: None,
            timer: None,
            read_stats_sender: None,
            cpu_stats_sender: None,
            collect_store_infos_interval: interval,
            load_base_split_check_interval: cmp::min(
                DEFAULT_LOAD_BASE_SPLIT_CHECK_INTERVAL,
                interval,
            ),
            // Use the smallest inspect latency as the minimal limitation for collecting tick.
            collect_tick_interval: cmp::min(
                get_minimal_inspect_tick_interval(
                    inspect_latency_interval,
                    inspect_kvdb_latency_interval,
                ),
                interval.min(default_collect_tick_interval()),
            ),
            inspect_latency_interval,
            inspect_kvdb_latency_interval,
            inspect_network_interval,
        }
    }

    // Collecting thread information and obtaining qps information for auto split.
    // They run together in the same thread by taking modulo at different intervals.
    pub fn start(
        &mut self,
        mut auto_split_controller: AutoSplitController,
        collector_reg_handle: CollectorRegHandle,
    ) -> Result<(), io::Error> {
        if self.collect_tick_interval
            < cmp::min(
                get_minimal_inspect_tick_interval(
                    self.inspect_latency_interval,
                    self.inspect_kvdb_latency_interval,
                ),
                default_collect_tick_interval(),
            )
        {
            info!(
                "interval is too small, skip stats monitoring. If we are running tests, it is normal, otherwise a check is needed."
            );
            return Ok(());
        }
        let mut timer_cnt = 0; // to run functions with different intervals in a loop
        let tick_interval = self.collect_tick_interval;
        let collect_store_infos_interval = self
            .collect_store_infos_interval
            .div_duration_f64(tick_interval) as u64;
        let load_base_split_check_interval = self
            .load_base_split_check_interval
            .div_duration_f64(tick_interval) as u64;
        let update_raftdisk_latency_stats_interval =
            self.inspect_latency_interval
                .div_duration_f64(tick_interval) as u64;
        let update_kvdisk_latency_stats_interval =
            self.inspect_kvdb_latency_interval
                .div_duration_f64(tick_interval) as u64;
        let update_network_latency_stats_interval =
            self.inspect_network_interval
                .div_duration_f64(tick_interval) as u64;

        let (timer_tx, timer_rx) = mpsc::channel();
        self.timer = Some(timer_tx);

        let (read_stats_sender, read_stats_receiver) =
            mpsc::sync_channel(STATS_CHANNEL_CAPACITY_LIMIT);
        self.read_stats_sender = Some(read_stats_sender);

        let (cpu_stats_sender, cpu_stats_receiver) =
            mpsc::sync_channel(STATS_CHANNEL_CAPACITY_LIMIT);
        self.cpu_stats_sender = Some(cpu_stats_sender);

        let reporter = self.reporter.clone();
        let props = tikv_util::thread_group::current_properties();

        fn is_enable_tick(timer_cnt: u64, interval: u64) -> bool {
            interval != 0 && timer_cnt % interval == 0
        }
        let h = Builder::new()
            .name(thd_name!("stats-monitor"))
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);

                // Create different `ThreadInfoStatistics` for different purposes to
                // make sure the record won't be disturbed.
                let mut collect_store_infos_thread_stats = ThreadInfoStatistics::new();
                let mut load_base_split_thread_stats = ThreadInfoStatistics::new();
                let mut region_cpu_records_collector = None;
                let mut auto_split_controller_ctx =
                    AutoSplitControllerContext::new(STATS_CHANNEL_CAPACITY_LIMIT);
                // Register the region CPU records collector.
                if auto_split_controller
                    .cfg
                    .region_cpu_overload_threshold_ratio()
                    > 0.0
                {
                    region_cpu_records_collector =
                        Some(collector_reg_handle.register(Box::new(reporter.clone()), false));
                }
                while let Err(mpsc::RecvTimeoutError::Timeout) =
                    timer_rx.recv_timeout(tick_interval)
                {
                    if is_enable_tick(timer_cnt, collect_store_infos_interval) {
                        StatsMonitor::collect_store_infos(
                            &mut collect_store_infos_thread_stats,
                            &reporter,
                        );
                    }
                    if is_enable_tick(timer_cnt, load_base_split_check_interval) {
                        StatsMonitor::load_base_split(
                            &mut auto_split_controller,
                            &mut auto_split_controller_ctx,
                            &read_stats_receiver,
                            &cpu_stats_receiver,
                            &mut load_base_split_thread_stats,
                            &reporter,
                            &collector_reg_handle,
                            &mut region_cpu_records_collector,
                        );
                    }
                    if is_enable_tick(timer_cnt, update_raftdisk_latency_stats_interval) {
                        reporter.update_latency_stats(timer_cnt, InspectFactor::RaftDisk);
                    }
                    if is_enable_tick(timer_cnt, update_kvdisk_latency_stats_interval) {
                        reporter.update_latency_stats(timer_cnt, InspectFactor::KvDisk);
                    }
                    if is_enable_tick(timer_cnt, update_network_latency_stats_interval) {
                        reporter.update_latency_stats(timer_cnt, InspectFactor::Network);
                    }
                    timer_cnt += 1;
                }
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn collect_store_infos(thread_stats: &mut ThreadInfoStatistics, reporter: &T) {
        thread_stats.record();
        let cpu_usages = convert_record_pairs(thread_stats.get_cpu_usages());
        let read_io_rates = convert_record_pairs(thread_stats.get_read_io_rates());
        let write_io_rates = convert_record_pairs(thread_stats.get_write_io_rates());

        reporter.report_store_infos(cpu_usages, read_io_rates, write_io_rates);
    }

    pub fn load_base_split(
        auto_split_controller: &mut AutoSplitController,
        auto_split_controller_ctx: &mut AutoSplitControllerContext,
        read_stats_receiver: &Receiver<ReadStats>,
        cpu_stats_receiver: &Receiver<Arc<RawRecords>>,
        thread_stats: &mut ThreadInfoStatistics,
        reporter: &T,
        collector_reg_handle: &CollectorRegHandle,
        region_cpu_records_collector: &mut Option<CollectorGuard>,
    ) {
        let start_time = TiInstant::now();
        match auto_split_controller.refresh_and_check_cfg() {
            SplitConfigChange::UpdateRegionCpuCollector(is_register) => {
                // If it's a deregister task, just take and drop the original collector.
                if !is_register {
                    region_cpu_records_collector.take();
                } else {
                    region_cpu_records_collector.get_or_insert(
                        collector_reg_handle.register(Box::new(reporter.clone()), false),
                    );
                }
            }
            SplitConfigChange::Noop => {}
        }
        let (top_qps, split_infos) = auto_split_controller.flush(
            auto_split_controller_ctx,
            read_stats_receiver,
            cpu_stats_receiver,
            thread_stats,
        );
        auto_split_controller.clear();
        auto_split_controller_ctx.maybe_gc();
        reporter.auto_split(split_infos);
        for i in 0..TOP_N {
            if i < top_qps.len() {
                READ_QPS_TOPN
                    .with_label_values(&[&i.to_string()])
                    .set(top_qps[i] as f64);
            } else {
                READ_QPS_TOPN.with_label_values(&[&i.to_string()]).set(0.0);
            }
        }
        LOAD_BASE_SPLIT_DURATION_HISTOGRAM.observe(start_time.saturating_elapsed_secs());
    }

    pub fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            drop(self.timer.take());
            drop(self.read_stats_sender.take());
            drop(self.cpu_stats_sender.take());
            if let Err(e) = h.join() {
                error!("join stats collector failed"; "err" => ?e);
            }
        }
    }

    #[inline]
    pub fn maybe_send_read_stats(&self, read_stats: ReadStats) {
        if let Some(sender) = &self.read_stats_sender {
            if sender.try_send(read_stats).is_err() {
                debug!("send read_stats failed, are we shutting down or channel is full?")
            }
        }
    }

    #[inline]
    pub fn maybe_send_cpu_stats(&self, cpu_stats: &Arc<RawRecords>) {
        if let Some(sender) = &self.cpu_stats_sender {
            if sender.try_send(cpu_stats.clone()).is_err() {
                debug!("send region cpu info failed, are we shutting down or channel is full?")
            }
        }
    }
}

const HOTSPOT_KEY_RATE_THRESHOLD: u64 = 128;
const HOTSPOT_QUERY_RATE_THRESHOLD: u64 = 128;
const HOTSPOT_BYTE_RATE_THRESHOLD: u64 = 8 * 1024;
const HOTSPOT_REPORT_CAPACITY: usize = 1000;

// TODO: support dynamic configure threshold in future.
fn hotspot_key_report_threshold() -> u64 {
    fail_point!("mock_hotspot_threshold", |_| { 0 });

    HOTSPOT_KEY_RATE_THRESHOLD * 10
}

fn hotspot_byte_report_threshold() -> u64 {
    fail_point!("mock_hotspot_threshold", |_| { 0 });

    HOTSPOT_BYTE_RATE_THRESHOLD * 10
}

fn hotspot_query_num_report_threshold() -> u64 {
    fail_point!("mock_hotspot_threshold", |_| { 0 });

    HOTSPOT_QUERY_RATE_THRESHOLD * 10
}

/// Max limitation of delayed store_heartbeat.
const STORE_HEARTBEAT_DELAY_LIMIT: u64 = 5 * 60;

pub struct Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    store_id: u64,
    pd_client: Arc<T>,
    router: RaftRouter<EK, ER>,
    region_peers: Arc<RwLock<HashMap<u64, PeerStat>>>,
    region_buckets: HashMap<u64, ReportBucket>,
    store_stat: StoreStat,
    is_hb_receiver_scheduled: bool,
    // Records the boot time.
    start_ts: UnixSecs,

    // use for Runner inner handle function to send Task to itself
    // actually it is the sender connected to Runner's Worker which
    // calls Runner's run() on Task received.
    scheduler: Scheduler<Task<EK>>,
    stats_monitor: StatsMonitor<WrappedScheduler<EK>>,
    store_heartbeat_interval: Duration,

    // region_id -> total_cpu_time_ms (since last region heartbeat)
    region_cpu_records: HashMap<u64, u32>,

    concurrency_manager: ConcurrencyManager,
    snap_mgr: SnapManager,
    remote: Remote<yatp::task::future::TaskCell>,

    health_reporter: RaftstoreReporter,
    health_controller: HealthController,

    coprocessor_host: CoprocessorHost<EK>,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2

    // Service manager for grpc service.
    grpc_service_manager: GrpcServiceManager,
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn new(
        cfg: &Config,
        store_id: u64,
        pd_client: Arc<T>,
        router: RaftRouter<EK, ER>,
        scheduler: Scheduler<Task<EK>>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
        snap_mgr: SnapManager,
        remote: Remote<yatp::task::future::TaskCell>,
        collector_reg_handle: CollectorRegHandle,
        health_controller: HealthController,
        coprocessor_host: CoprocessorHost<EK>,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        grpc_service_manager: GrpcServiceManager,
    ) -> Runner<EK, ER, T> {
        let mut store_stat = StoreStat::default();
        store_stat.set_cpu_quota(SysQuota::cpu_cores_quota(), cfg.inspect_cpu_util_thd);
        let store_heartbeat_interval = cfg.pd_store_heartbeat_tick_interval.0;
        let interval = store_heartbeat_interval / NUM_COLLECT_STORE_INFOS_PER_HEARTBEAT;
        let mut stats_monitor = StatsMonitor::new(
            interval,
            cfg.inspect_interval.0,
            cfg.inspect_kvdb_interval.0,
            cfg.inspect_network_interval.0,
            WrappedScheduler(scheduler.clone()),
        );
        if let Err(e) = stats_monitor.start(auto_split_controller, collector_reg_handle) {
            error!("failed to start stats collector, error = {:?}", e);
        }

        let health_reporter_config: RaftstoreReporterConfig = RaftstoreReporterConfig {
            inspect_interval: cfg.inspect_interval.0,
            inspect_kvdb_interval: cfg.inspect_kvdb_interval.0,
            inspect_network_interval: cfg.inspect_network_interval.0,

            unsensitive_cause: cfg.slow_trend_unsensitive_cause,
            unsensitive_result: cfg.slow_trend_unsensitive_result,
            net_io_factor: cfg.slow_trend_network_io_factor,

            cause_spike_filter_value_gauge: STORE_SLOW_TREND_MISC_GAUGE_VEC
                .with_label_values(&["spike_filter_value"]),
            cause_spike_filter_count_gauge: STORE_SLOW_TREND_MISC_GAUGE_VEC
                .with_label_values(&["spike_filter_count"]),
            cause_l1_gap_gauges: STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                .with_label_values(&["L1"]),
            cause_l2_gap_gauges: STORE_SLOW_TREND_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                .with_label_values(&["L2"]),
            result_spike_filter_value_gauge: STORE_SLOW_TREND_RESULT_MISC_GAUGE_VEC
                .with_label_values(&["spike_filter_value"]),
            result_spike_filter_count_gauge: STORE_SLOW_TREND_RESULT_MISC_GAUGE_VEC
                .with_label_values(&["spike_filter_count"]),
            result_l1_gap_gauges: STORE_SLOW_TREND_RESULT_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                .with_label_values(&["L1"]),
            result_l2_gap_gauges: STORE_SLOW_TREND_RESULT_MARGIN_ERROR_WINDOW_GAP_GAUGE_VEC
                .with_label_values(&["L2"]),
        };

        let health_reporter = RaftstoreReporter::new(&health_controller, health_reporter_config);

        Runner {
            store_id,
            pd_client,
            router,
            is_hb_receiver_scheduled: false,
            region_peers: Arc::new(RwLock::new(HashMap::default())),
            region_buckets: HashMap::default(),
            store_stat,
            start_ts: UnixSecs::now(),
            scheduler,
            store_heartbeat_interval,
            stats_monitor,
            region_cpu_records: HashMap::default(),
            concurrency_manager,
            snap_mgr,
            remote,
            health_reporter,
            health_controller,
            coprocessor_host,
            causal_ts_provider,
            grpc_service_manager,
        }
    }

    // Deprecate
    fn handle_ask_split(
        &self,
        mut region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
        right_derive: bool,
        share_source_region_size: bool,
        callback: Callback<EK::Snapshot>,
        task: String,
    ) {
        let router = self.router.clone();
        let resp = self.pd_client.ask_split(region.clone());
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    info!(
                        "try to split region";
                        "region_id" => region.get_id(),
                        "new_region_id" => resp.get_new_region_id(),
                        "region" => ?region,
                        "task"=>task,
                    );

                    let req = new_split_region_request(
                        split_key,
                        resp.get_new_region_id(),
                        resp.take_new_peer_ids(),
                        right_derive,
                        share_source_region_size,
                    );
                    let region_id = region.get_id();
                    let epoch = region.take_region_epoch();
                    send_admin_request(
                        &router,
                        region_id,
                        epoch,
                        peer,
                        req,
                        callback,
                        Default::default(),
                    );
                }
                Err(e) => {
                    warn!("failed to ask split";
                    "region_id" => region.get_id(),
                    "err" => ?e,
                    "task"=>task);
                }
            }
        };
        self.remote.spawn(f);
    }

    // Note: The parameter doesn't contain `self` because this function may
    // be called in an asynchronous context.
    fn handle_ask_batch_split(
        router: RaftRouter<EK, ER>,
        scheduler: Scheduler<Task<EK>>,
        pd_client: Arc<T>,
        mut region: metapb::Region,
        mut split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
        share_source_region_size: bool,
        callback: Callback<EK::Snapshot>,
        task: String,
        remote: Remote<yatp::task::future::TaskCell>,
    ) {
        if split_keys.is_empty() {
            info!("empty split key, skip ask batch split";
                "region_id" => region.get_id());
            return;
        }
        let resp = pd_client.ask_batch_split(region.clone(), split_keys.len());
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    info!(
                        "try to batch split region";
                        "region_id" => region.get_id(),
                        "new_region_ids" => ?resp.get_ids(),
                        "region" => ?region,
                        "task" => task,
                    );

                    let req = new_batch_split_region_request(
                        split_keys,
                        resp.take_ids().into(),
                        right_derive,
                        share_source_region_size,
                    );
                    let region_id = region.get_id();
                    let epoch = region.take_region_epoch();
                    send_admin_request(
                        &router,
                        region_id,
                        epoch,
                        peer,
                        req,
                        callback,
                        Default::default(),
                    );
                }
                // When rolling update, there might be some old version tikvs that don't support
                // batch split in cluster. In this situation, PD version check would refuse
                // `ask_batch_split`. But if update time is long, it may cause large Regions, so
                // call `ask_split` instead.
                Err(Error::Incompatible) => {
                    let (region_id, peer_id) = (region.id, peer.id);
                    info!(
                        "ask_batch_split is incompatible, use ask_split instead";
                        "region_id" => region_id
                    );
                    let task = Task::AskSplit {
                        region,
                        split_key: split_keys.pop().unwrap(),
                        peer,
                        right_derive,
                        share_source_region_size,
                        callback,
                    };
                    if let Err(ScheduleError::Stopped(t)) = scheduler.schedule(task) {
                        error!(
                            "failed to notify pd to split: Stopped";
                            "region_id" => region_id,
                            "peer_id" =>  peer_id
                        );
                        match t {
                            Task::AskSplit { callback, .. } => {
                                callback.invoke_with_response(new_error(box_err!(
                                    "failed to split: Stopped"
                                )));
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "ask batch split failed";
                        "region_id" => region.get_id(),
                        "err" => ?e,
                    );
                }
            }
        };
        remote.spawn(f);
    }

    fn handle_heartbeat(
        &self,
        term: u64,
        region: metapb::Region,
        peer: metapb::Peer,
        region_stat: RegionStat,
        replication_status: Option<RegionReplicationStatus>,
    ) {
        self.store_stat
            .region_bytes_written
            .observe(region_stat.written_bytes as f64);
        self.store_stat
            .region_keys_written
            .observe(region_stat.written_keys as f64);
        self.store_stat
            .region_bytes_read
            .observe(region_stat.read_bytes as f64);
        self.store_stat
            .region_keys_read
            .observe(region_stat.read_keys as f64);

        self.coprocessor_host
            .on_region_heartbeat(&region, &region_stat);
        let resp = self.pd_client.region_heartbeat(
            term,
            region.clone(),
            peer,
            region_stat,
            replication_status,
        );
        let f = async move {
            if let Err(e) = resp.await {
                debug!(
                    "failed to send heartbeat";
                    "region_id" => region.get_id(),
                    "err" => ?e
                );
            }
        };
        self.remote.spawn(f);
    }

    fn handle_store_heartbeat(
        &mut self,
        mut stats: pdpb::StoreStats,
        is_fake_heartbeat: bool,
        store_report: Option<pdpb::StoreReport>,
        dr_autosync_status: Option<StoreDrAutoSyncStatus>,
    ) {
        let mut report_peers = HashMap::default();
        {
            let mut region_peers = self.region_peers.write().unwrap();
            for (region_id, region_peer) in region_peers.iter_mut() {
                let read_bytes = region_peer.read_bytes - region_peer.last_store_report_read_bytes;
                let read_keys = region_peer.read_keys - region_peer.last_store_report_read_keys;
                let query_stats = region_peer
                    .query_stats
                    .sub_query_stats(&region_peer.last_store_report_query_stats);
                region_peer.last_store_report_read_bytes = region_peer.read_bytes;
                region_peer.last_store_report_read_keys = region_peer.read_keys;
                region_peer
                    .last_store_report_query_stats
                    .fill_query_stats(&region_peer.query_stats);
                if read_bytes < hotspot_byte_report_threshold()
                    && read_keys < hotspot_key_report_threshold()
                    && query_stats.get_read_query_num() < hotspot_query_num_report_threshold()
                {
                    continue;
                }
                let mut read_stat = pdpb::PeerStat::default();
                read_stat.set_region_id(*region_id);
                read_stat.set_read_keys(read_keys);
                read_stat.set_read_bytes(read_bytes);
                read_stat.set_query_stats(query_stats.0);
                report_peers.insert(*region_id, read_stat);
            }
        }

        stats = collect_report_read_peer_stats(HOTSPOT_REPORT_CAPACITY, report_peers, stats);
        // Fetch all size infos and update last reported infos on engine_size.
        let (capacity, used_size, available) = collect_engine_size(&self.coprocessor_host);
        if available == 0 {
            warn!("no available space");
        }
        self.store_stat.engine_last_capacity_size = capacity;
        self.store_stat.engine_last_used_size = used_size;
        self.store_stat.engine_last_available_size = available;

        stats.set_capacity(capacity);
        stats.set_used_size(used_size);
        stats.set_available(available);
        stats.set_bytes_read(
            self.store_stat.engine_total_bytes_read - self.store_stat.engine_last_total_bytes_read,
        );
        stats.set_keys_read(
            self.store_stat.engine_total_keys_read - self.store_stat.engine_last_total_keys_read,
        );

        self.store_stat
            .engine_total_query_num
            .add_query_stats(stats.get_query_stats()); // add write query stat
        let res = self
            .store_stat
            .engine_total_query_num
            .sub_query_stats(&self.store_stat.engine_last_query_num);
        let all_query_num = res.get_all_query_num();
        stats.set_query_stats(res.0);

        stats.set_cpu_usages(self.store_stat.store_cpu_usages.clone().into());
        stats.set_read_io_rates(self.store_stat.store_read_io_rates.clone().into());
        stats.set_write_io_rates(self.store_stat.store_write_io_rates.clone().into());

        let mut interval = pdpb::TimeInterval::default();
        interval.set_start_timestamp(self.store_stat.last_report_ts.into_inner());
        stats.set_interval(interval);
        self.store_stat.engine_last_total_bytes_read = self.store_stat.engine_total_bytes_read;
        self.store_stat.engine_last_total_keys_read = self.store_stat.engine_total_keys_read;
        self.store_stat
            .engine_last_query_num
            .fill_query_stats(&self.store_stat.engine_total_query_num);
        self.store_stat.last_report_ts = if !is_fake_heartbeat {
            UnixSecs::now()
        } else {
            // If `is_fake_heartbeat == true`, the given Task::StoreHeartbeat should be a
            // fake heartbeat to PD, we won't update the last_report_ts to avoid
            // incorrectly marking current TiKV node in normal state.
            self.store_stat.last_report_ts
        };
        self.store_stat.region_bytes_written.flush();
        self.store_stat.region_keys_written.flush();
        self.store_stat.region_bytes_read.flush();
        self.store_stat.region_keys_read.flush();

        stats.set_slow_score(self.health_reporter.get_disk_slow_score() as u64);
        // Filter out network slow scores equal to 1 to reduce message volume
        let network_scores = self
            .health_reporter
            .get_network_slow_score()
            .into_iter()
            .filter(|(_, score)| *score != 1)
            .collect();
        stats.set_network_slow_scores(network_scores);

        let (rps, slow_trend_pb) = self
            .health_reporter
            .update_slow_trend(all_query_num, Instant::now());
        self.flush_slow_trend_metrics(rps, &slow_trend_pb);
        stats.set_slow_trend(slow_trend_pb);

        stats.set_is_grpc_paused(self.grpc_service_manager.is_paused());

        let scheduler = self.scheduler.clone();
        let router = self.router.clone();
        let region_peers = self.region_peers.clone();
        let mut snap_mgr = self.snap_mgr.clone();
        let resp = self
            .pd_client
            .store_heartbeat(stats, store_report, dr_autosync_status);
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    if GLOBAL_SERVER_READINESS
                        .connected_to_pd
                        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        // Log when the server readiness condition changes.
                        info!("ServerReadiness: connected to PD");
                    }

                    if let Some(status) = resp.replication_status.take() {
                        let _ = router.send_control(StoreMsg::UpdateReplicationMode(status));
                    }
                    if let Some(mut plan) = resp.recovery_plan.take() {
                        info!("Unsafe recovery, received a recovery plan");
                        let handle = Arc::new(Mutex::new(router.clone()));
                        if plan.has_force_leader() {
                            let mut failed_stores = HashSet::default();
                            for failed_store in plan.get_force_leader().get_failed_stores() {
                                failed_stores.insert(*failed_store);
                            }
                            let syncer = UnsafeRecoveryForceLeaderSyncer::new(
                                plan.get_step(),
                                handle.clone(),
                            );
                            for region in plan.get_force_leader().get_enter_force_leaders() {
                                if let Err(e) = handle.send_enter_force_leader(
                                    *region,
                                    syncer.clone(),
                                    failed_stores.clone(),
                                ) {
                                    error!("fail to send force leader message for recovery"; "err" => ?e);
                                }
                            }
                        } else {
                            let syncer = UnsafeRecoveryExecutePlanSyncer::new(
                                plan.get_step(),
                                handle.clone(),
                            );
                            for create in plan.take_creates().into_iter() {
                                if let Err(e) = handle.send_create_peer(create, syncer.clone()) {
                                    error!("fail to send create peer message for recovery"; "err" => ?e);
                                }
                            }
                            for delete in plan.take_tombstones().into_iter() {
                                if let Err(e) = handle.send_destroy_peer(delete, syncer.clone()) {
                                    error!("fail to send destroy peer message for recovery"; "err" => ?e);
                                }
                            }
                            for mut demote in plan.take_demotes().into_iter() {
                                if let Err(e) = handle.send_demote_peers(
                                    demote.get_region_id(),
                                    demote.take_failed_voters().into_vec(),
                                    syncer.clone(),
                                ) {
                                    error!("fail to send update peer list message for recovery"; "err" => ?e);
                                }
                            }
                        }
                    }
                    // Force awaken all hibernated regions if there are slow stores in this
                    // cluster. To mitigate the impact of stalls when awakening too many regions,
                    // we break up all regions into small batches.
                    if let Some(awaken_regions) = resp.awaken_regions.take() {
                        info!("forcely awaken hibernated regions in this store");
                        let abnormal_stores = awaken_regions.get_abnormal_stores().to_vec();
                        // The chunk_size of 1024 is an empirical batch size that balances
                        // between generating too many `StoreMsg::AwakenRegions` messages and
                        // keeping each batch small enough to avoid stalling Raftstore for
                        // extended periods.
                        let chunk_size = 1024;
                        let mut region_ids = Vec::with_capacity(chunk_size);

                        for (id, _) in region_peers.read().unwrap().iter() {
                            region_ids.push(*id);

                            // Send batch when it reaches the chunk size
                            if region_ids.len() >= chunk_size {
                                let _ = router.send_store_msg(StoreMsg::AwakenRegions {
                                    abnormal_stores: abnormal_stores.clone(),
                                    region_ids: std::mem::take(&mut region_ids),
                                });
                                region_ids.reserve(chunk_size);
                            }
                        }

                        // Send remaining regions if any
                        if !region_ids.is_empty() {
                            let _ = router.send_store_msg(StoreMsg::AwakenRegions {
                                abnormal_stores: abnormal_stores.clone(),
                                region_ids,
                            });
                        }
                    }
                    // Control grpc server.
                    if let Some(op) = resp.control_grpc.take() {
                        if let Err(e) =
                            scheduler.schedule(Task::ControlGrpcServer(op.get_ctrl_event()))
                        {
                            warn!("fail to schedule control grpc task"; "err" => ?e);
                        }
                    }
                    // NodeState for this store.
                    {
                        let state = (|| {
                            #[cfg(feature = "failpoints")]
                            fail_point!("manually_set_store_offline", |_| {
                                metapb::NodeState::Removing
                            });
                            resp.get_state()
                        })();
                        match state {
                            metapb::NodeState::Removing | metapb::NodeState::Removed => {
                                let is_offlined = snap_mgr.is_offlined();
                                if !is_offlined {
                                    snap_mgr.set_offline(true);
                                    info!("store is offlined by pd");
                                }
                            }
                            // As for NodeState::Preparing | Serving, if `is_offlined == true`,
                            // it means the store has been re-added into the cluster and
                            // the offline operation is terminated. Therefore, the state
                            // `is_offlined` should be reset with `false`.
                            _ => {
                                let is_offlined = snap_mgr.is_offlined();
                                if is_offlined {
                                    snap_mgr.set_offline(false);
                                    info!("store is remarked with serving state by pd");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("store heartbeat failed"; "err" => ?e);
                }
            }
        };
        self.remote.spawn(f);
    }

    fn flush_slow_trend_metrics(
        &mut self,
        requests_per_sec: Option<f64>,
        slow_trend_pb: &pdpb::SlowTrend,
    ) {
        let slow_trend = self.health_reporter.get_slow_trend();
        // Latest result.
        STORE_SLOW_TREND_GAUGE.set(slow_trend_pb.get_cause_rate());
        if let Some(requests_per_sec) = requests_per_sec {
            STORE_SLOW_TREND_RESULT_GAUGE.set(slow_trend_pb.get_result_rate());
            STORE_SLOW_TREND_RESULT_VALUE_GAUGE.set(requests_per_sec);
        } else {
            // Just to mark the invalid range on the graphic
            STORE_SLOW_TREND_RESULT_VALUE_GAUGE.set(-100.0);
        }

        // Current internal states.
        STORE_SLOW_TREND_L0_GAUGE.set(slow_trend.slow_cause.l0_avg());
        STORE_SLOW_TREND_L1_GAUGE.set(slow_trend.slow_cause.l1_avg());
        STORE_SLOW_TREND_L2_GAUGE.set(slow_trend.slow_cause.l2_avg());
        STORE_SLOW_TREND_L0_L1_GAUGE.set(slow_trend.slow_cause.l0_l1_rate());
        STORE_SLOW_TREND_L1_L2_GAUGE.set(slow_trend.slow_cause.l1_l2_rate());
        STORE_SLOW_TREND_L1_MARGIN_ERROR_GAUGE.set(slow_trend.slow_cause.l1_margin_error_base());
        STORE_SLOW_TREND_L2_MARGIN_ERROR_GAUGE.set(slow_trend.slow_cause.l2_margin_error_base());
        // Report results of all slow Trends.
        STORE_SLOW_TREND_RESULT_L0_GAUGE.set(slow_trend.slow_result.l0_avg());
        STORE_SLOW_TREND_RESULT_L1_GAUGE.set(slow_trend.slow_result.l1_avg());
        STORE_SLOW_TREND_RESULT_L2_GAUGE.set(slow_trend.slow_result.l2_avg());
        STORE_SLOW_TREND_RESULT_L0_L1_GAUGE.set(slow_trend.slow_result.l0_l1_rate());
        STORE_SLOW_TREND_RESULT_L1_L2_GAUGE.set(slow_trend.slow_result.l1_l2_rate());
        STORE_SLOW_TREND_RESULT_L1_MARGIN_ERROR_GAUGE
            .set(slow_trend.slow_result.l1_margin_error_base());
        STORE_SLOW_TREND_RESULT_L2_MARGIN_ERROR_GAUGE
            .set(slow_trend.slow_result.l2_margin_error_base());
    }

    fn handle_report_batch_split(&self, regions: Vec<metapb::Region>) {
        let resp = self.pd_client.report_batch_split(regions);
        let f = async move {
            if let Err(e) = resp.await {
                warn!("report split failed"; "err" => ?e);
            }
        };
        self.remote.spawn(f);
    }

    fn handle_validate_peer(&self, local_region: metapb::Region, peer: metapb::Peer) {
        let router = self.router.clone();
        let resp = self.pd_client.get_region_by_id(local_region.get_id());
        let f = async move {
            match resp.await {
                Ok(Some(pd_region)) => {
                    if is_epoch_stale(
                        pd_region.get_region_epoch(),
                        local_region.get_region_epoch(),
                    ) {
                        // The local Region epoch is fresher than Region epoch in PD
                        // This means the Region info in PD is not updated to the latest even
                        // after `max_leader_missing_duration`. Something is wrong in the system.
                        // Just add a log here for this situation.
                        info!(
                            "local region epoch is greater the \
                             region epoch in PD ignore validate peer";
                            "region_id" => local_region.get_id(),
                            "peer_id" => peer.get_id(),
                            "local_region_epoch" => ?local_region.get_region_epoch(),
                            "pd_region_epoch" => ?pd_region.get_region_epoch()
                        );
                        PD_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["region epoch error"])
                            .inc();
                        return;
                    }

                    if pd_region
                        .get_peers()
                        .iter()
                        .all(|p| p.get_id() != peer.get_id())
                    {
                        // Peer is not a member of this Region anymore. Probably it's removed out.
                        // Send it a raft massage to destroy it since it's obsolete.
                        info!(
                            "peer is not a valid member of region, to be \
                             destroyed soon";
                            "region_id" => local_region.get_id(),
                            "peer_id" => peer.get_id(),
                            "pd_region" => ?pd_region
                        );
                        PD_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["peer stale"])
                            .inc();
                        send_destroy_peer_message(&router, local_region, peer, pd_region);
                    } else {
                        info!(
                            "peer is still a valid member of region";
                            "region_id" => local_region.get_id(),
                            "peer_id" => peer.get_id(),
                            "pd_region" => ?pd_region
                        );
                        PD_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["peer valid"])
                            .inc();
                    }
                }
                Ok(None) => {
                    // Splitted region has not yet reported to PD.
                    //
                    // Or region has been merged. This case is handled by
                    // message `MsgCheckStalePeer`, stale peers will be
                    // removed eventually.
                    PD_VALIDATE_PEER_COUNTER_VEC
                        .with_label_values(&["region not found"])
                        .inc();
                }
                Err(e) => {
                    warn!("get region failed"; "err" => ?e);
                }
            }
        };
        self.remote.spawn(f);
    }

    fn schedule_heartbeat_receiver(&mut self) {
        let router = self.router.clone();
        let store_id = self.store_id;

        let fut = self.pd_client
            .handle_region_heartbeat_response(self.store_id, move |mut resp| {
                let region_id = resp.get_region_id();
                let epoch = resp.take_region_epoch();
                let peer = resp.take_target_peer();

                if resp.has_change_peer() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["change peer"])
                        .inc();

                    let mut change_peer = resp.take_change_peer();
                    info!(
                        "try to change peer";
                        "region_id" => region_id,
                        "change_type" => ?change_peer.get_change_type(),
                        "peer" => ?change_peer.get_peer()
                    );
                    let req = new_change_peer_request(
                        change_peer.get_change_type(),
                        change_peer.take_peer(),
                    );
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None, Default::default());
                } else if resp.has_change_peer_v2() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["change peer"])
                        .inc();

                    let mut change_peer_v2 = resp.take_change_peer_v2();
                    info!(
                        "try to change peer";
                        "region_id" => region_id,
                        "changes" => ?change_peer_v2.get_changes(),
                    );
                    let req = new_change_peer_v2_request(change_peer_v2.take_changes().into());
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None, Default::default());
                } else if resp.has_transfer_leader() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["transfer leader"])
                        .inc();

                    let mut transfer_leader = resp.take_transfer_leader();
                    info!(
                        "try to transfer leader";
                        "region_id" => region_id,
                        "from_peer" => ?peer,
                        "to_peer" => ?transfer_leader.get_peer(),
                        "to_peers" => ?transfer_leader.get_peers(),
                    );
                    let req = new_transfer_leader_request(transfer_leader.take_peer(), transfer_leader.take_peers().into());
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None, Default::default());
                } else if resp.has_split_region() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["split region"])
                        .inc();

                    let mut split_region = resp.take_split_region();
                    info!("try to split"; "region_id" => region_id, "region_epoch" => ?epoch);
                    let msg = if split_region.get_policy() == pdpb::CheckPolicy::Usekey {
                        CasualMessage::SplitRegion {
                            region_epoch: epoch,
                            split_keys: split_region.take_keys().into(),
                            callback: Callback::None,
                            source: "pd".into(),
                            share_source_region_size: false,
                        }
                    } else {
                        CasualMessage::HalfSplitRegion {
                            region_epoch: epoch,
                            start_key: None,
                            end_key: None,
                            policy: split_region.get_policy(),
                            source: "pd",
                            cb: Callback::None,
                        }
                    };
                    if let Err(e) = router.send(region_id, PeerMsg::CasualMessage(Box::new(msg))) {
                        error!("send halfsplit request failed"; "region_id" => region_id, "err" => ?e);
                    }
                } else if resp.has_merge() {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["merge"]).inc();

                    let merge = resp.take_merge();
                    info!("try to merge"; "region_id" => region_id, "merge" => ?merge);
                    let req = new_merge_request(merge);
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None, RaftCmdExtraOpts{
                        deadline:None,
                        disk_full_opt:DiskFullOpt::AllowedOnAlmostFull,
                    });
                } else if resp.has_switch_witnesses() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["switch witness"])
                        .inc();

                    let mut switches = resp.take_switch_witnesses();
                    info!("try to switch witness";
                          "region_id" => region_id,
                          "switch_witness" => ?switches
                    );
                    let req = new_batch_switch_witness(switches.take_switch_witnesses().into());
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None, Default::default());
                } else {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["noop"]).inc();
                }
            });
        let f = async move {
            match fut.await {
                Ok(_) => {
                    info!(
                        "region heartbeat response handler exit";
                        "store_id" => store_id,
                    );
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        };
        self.remote.spawn(f);
        self.is_hb_receiver_scheduled = true;
    }

    fn handle_read_stats(&mut self, mut read_stats: ReadStats) {
        for (region_id, region_info) in read_stats.region_infos.iter_mut() {
            {
                let mut region_peers = self.region_peers.write().unwrap();
                let peer_stat = region_peers.entry(*region_id).or_default();
                peer_stat.read_bytes += region_info.flow.read_bytes as u64;
                peer_stat.read_keys += region_info.flow.read_keys as u64;
                self.store_stat.engine_total_bytes_read += region_info.flow.read_bytes as u64;
                self.store_stat.engine_total_keys_read += region_info.flow.read_keys as u64;
                peer_stat
                    .query_stats
                    .add_query_stats(&region_info.query_stats.0);
                peer_stat.cop_detail.add(&region_info.cop_detail);
            }
            self.store_stat
                .engine_total_query_num
                .add_query_stats(&region_info.query_stats.0);
        }
        for (_, region_buckets) in mem::take(&mut read_stats.region_buckets) {
            self.merge_buckets(region_buckets);
        }
        if !read_stats.region_infos.is_empty() {
            self.stats_monitor.maybe_send_read_stats(read_stats);
        }
    }

    fn handle_write_stats(&mut self, mut write_stats: WriteStats) {
        for (region_id, region_info) in write_stats.region_infos.iter_mut() {
            {
                let mut region_peers = self.region_peers.write().unwrap();
                let peer_stat = region_peers.entry(*region_id).or_default();
                peer_stat.query_stats.add_query_stats(&region_info.0);
            }
            self.store_stat
                .engine_total_query_num
                .add_query_stats(&region_info.0);
        }
    }

    fn handle_destroy_peer(&mut self, region_id: u64) {
        match self.region_peers.write().unwrap().remove(&region_id) {
            None => {}
            Some(_) => info!("remove peer statistic record in pd"; "region_id" => region_id),
        }
    }

    fn handle_store_infos(
        &mut self,
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    ) {
        self.store_stat.store_cpu_usages = cpu_usages;
        self.store_stat.store_read_io_rates = read_io_rates;
        self.store_stat.store_write_io_rates = write_io_rates;
    }

    fn handle_update_max_timestamp(
        &mut self,
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    ) {
        let pd_client = self.pd_client.clone();
        let concurrency_manager = self.concurrency_manager.clone();
        let causal_ts_provider = self.causal_ts_provider.clone();
        let log_interval = Duration::from_secs(5);
        let mut last_log_ts = Instant::now().checked_sub(log_interval).unwrap();

        let f = async move {
            let mut success = false;
            while txn_ext.max_ts_sync_status.load(Ordering::SeqCst) == initial_status {
                // On leader transfer / region merge, RawKV API v2 need to invoke
                // causal_ts_provider.flush() to renew cached TSO, to ensure that
                // the next TSO returned by causal_ts_provider.get_ts() on current
                // store must be larger than the store where the leader is on before.
                //
                // And it won't break correctness of transaction commands, as
                // causal_ts_provider.flush() is implemented as pd_client.get_tso() + renew TSO
                // cached.
                let res: crate::Result<()> =
                    if let Some(causal_ts_provider) = &causal_ts_provider {
                        causal_ts_provider
                            .async_flush()
                            .await
                            .map_err(|e| box_err!(e))
                    } else {
                        pd_client.get_tso().await.map_err(Into::into)
                    }
                    .and_then(|ts| {
                        concurrency_manager
                            .update_max_ts(ts, "raftstore")
                            .map_err(|e| crate::Error::Other(box_err!(e)))
                    });

                match res {
                    Ok(()) => {
                        // Set the least significant bit to 1 to mark it as synced.
                        success = txn_ext
                            .max_ts_sync_status
                            .compare_exchange(
                                initial_status,
                                initial_status | 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok();
                        break;
                    }
                    Err(e) => {
                        if last_log_ts.elapsed() > log_interval {
                            warn!(
                                "failed to update max timestamp for region";
                                "region_id" => region_id,
                                "error" => ?e
                            );
                            last_log_ts = Instant::now();
                        }
                    }
                }
            }
            if success {
                info!("succeed to update max timestamp"; "region_id" => region_id);
            } else {
                info!(
                    "updating max timestamp is stale";
                    "region_id" => region_id,
                    "initial_status" => initial_status,
                );
            }
        };

        #[cfg(feature = "failpoints")]
        let delay = (|| {
            fail_point!("delay_update_max_ts", |_| true);
            false
        })();
        #[cfg(not(feature = "failpoints"))]
        let delay = false;

        if delay {
            info!("[failpoint] delay update max ts for 1s"; "region_id" => region_id);
            let deadline = Instant::now() + Duration::from_secs(1);
            self.remote
                .spawn(GLOBAL_TIMER_HANDLE.delay(deadline).compat().then(|_| f));
        } else {
            self.remote.spawn(f);
        }
    }

    fn handle_query_region_leader(&self, region_id: u64) {
        let router = self.router.clone();
        let resp = self.pd_client.get_region_leader_by_id(region_id);
        let f = async move {
            match resp.await {
                Ok(Some((region, leader))) => {
                    if leader.get_store_id() != 0 {
                        let msg = Box::new(CasualMessage::QueryRegionLeaderResp { region, leader });
                        if let Err(e) = router.send(region_id, PeerMsg::CasualMessage(msg)) {
                            warn!("send region info message failed"; "region_id" => region_id, "err" => ?e);
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("get region failed"; "err" => ?e);
                }
            }
        };
        self.remote.spawn(f);
    }

    // Notice: CPU records here we collect are all from the outside RPC workloads,
    // CPU consumption from internal TiKV are not included. Also, since the write
    // path CPU consumption is not large but the logging is complex, the current
    // CPU time for the write path only takes into account the lock checking,
    // which is the read load portion of the write path.
    // TODO: more accurate CPU consumption of a specified region.
    fn handle_region_cpu_records(&mut self, records: Arc<RawRecords>) {
        // Send Region CPU info to AutoSplitController inside the stats_monitor.
        self.stats_monitor.maybe_send_cpu_stats(&records);
        calculate_region_cpu_records(self.store_id, records, &mut self.region_cpu_records);
    }

    fn handle_report_min_resolved_ts(&self, store_id: u64, min_resolved_ts: u64) {
        let resp = self
            .pd_client
            .report_min_resolved_ts(store_id, min_resolved_ts);
        let f = async move {
            if let Err(e) = resp.await {
                warn!("report min resolved_ts failed"; "err" => ?e);
            }
        };
        self.remote.spawn(f);
    }

    fn handle_report_region_buckets(&mut self, region_buckets: BucketStat) {
        let region_id = region_buckets.meta.region_id;
        self.merge_buckets(region_buckets);
        let report_buckets = self.region_buckets.get_mut(&region_id).unwrap();
        let last_report_ts = if report_buckets.last_report_ts.is_zero() {
            self.start_ts
        } else {
            report_buckets.last_report_ts
        };
        let now = UnixSecs::now();
        let interval_second = now.into_inner() - last_report_ts.into_inner();
        let delta = report_buckets.new_report(now);
        let resp = self
            .pd_client
            .report_region_buckets(&delta, Duration::from_secs(interval_second));
        let f = async move {
            // Migrated to 2021 migration. This let statement is probably not needed, see
            //   https://doc.rust-lang.org/edition-guide/rust-2021/disjoint-capture-in-closures.html
            let _ = &delta;
            if let Err(e) = resp.await {
                debug!(
                    "failed to send buckets";
                    "region_id" => region_id,
                    "version" => delta.meta.version,
                    "region_epoch" => ?delta.meta.region_epoch,
                    "err" => ?e
                );
            }
        };
        self.remote.spawn(f);
    }

    fn merge_buckets(&mut self, mut buckets: BucketStat) {
        let region_id = buckets.meta.region_id;
        self.region_buckets
            .entry(region_id)
            .and_modify(|report_bucket| {
                let current = &mut report_bucket.current_stat;
                if current.meta < buckets.meta {
                    mem::swap(current, &mut buckets);
                }
                current.merge(&buckets);
            })
            .or_insert_with(|| ReportBucket::new(buckets));
    }

    /// Force to send a special heartbeat to pd when current store is hung on
    /// some special circumstances, i.e. disk busy, handler busy and others.
    fn handle_fake_store_heartbeat(&mut self) {
        let mut stats = pdpb::StoreStats::default();
        stats.set_store_id(self.store_id);
        stats.set_region_count(self.region_peers.read().unwrap().len() as u32);

        let snap_stats = self.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

        stats.set_start_time(self.start_ts.into_inner() as u32);

        // This calling means that the current node cannot report heartbeat in normaly
        // scheduler. That is, the current node must in `busy` state.
        stats.set_is_busy(true);

        // We do not need to report store_info, so we just set `None` here.
        self.handle_store_heartbeat(stats, true, None, None);
        warn!("scheduling store_heartbeat timeout, force report store slow score to pd.";
            "store_id" => self.store_id,
        );
    }

    fn is_store_heartbeat_delayed(&self) -> bool {
        let now = UnixSecs::now();
        let interval_second = now.into_inner() - self.store_stat.last_report_ts.into_inner();
        // Only if the `last_report_ts`, that is, the last timestamp of
        // store_heartbeat, exceeds the interval of store heartbaet but less than
        // the given limitation, will it trigger a report of fake heartbeat to
        // make the statistics of slowness percepted by PD timely.
        (interval_second > self.store_heartbeat_interval.as_secs())
            && (interval_second <= STORE_HEARTBEAT_DELAY_LIMIT)
    }

    fn handle_control_grpc_server(&mut self, event: pdpb::ControlGrpcEvent) {
        info!("forcely control grpc server";
                "curr_health_status" => ?self.health_controller.get_serving_status(),
                "event" => ?event,
        );
        match event {
            pdpb::ControlGrpcEvent::Pause => {
                if let Err(e) = self.grpc_service_manager.pause() {
                    warn!("failed to send service event to PAUSE grpc server";
                            "err" => ?e);
                } else {
                    self.health_controller.set_is_serving(false);
                }
            }
            pdpb::ControlGrpcEvent::Resume => {
                if let Err(e) = self.grpc_service_manager.resume() {
                    warn!("failed to send service event to RESUME grpc server";
                            "err" => ?e);
                } else {
                    self.health_controller.set_is_serving(true);
                }
            }
        }
    }

    fn handle_inspect_latency(&mut self, factor: InspectFactor) {
        let slow_score_tick_result = self
            .health_reporter
            .tick(self.store_stat.maybe_busy(), factor);
        if let Some(score) = slow_score_tick_result.updated_score {
            STORE_SLOW_SCORE_GAUGE
                .with_label_values(&[factor.as_str()])
                .set(score as i64);
        }
        let id = slow_score_tick_result.tick_id;
        let scheduler = self.scheduler.clone();

        let inspector = {
            match factor {
                InspectFactor::RaftDisk => {
                    // If the last slow_score already reached abnormal state and was delayed for
                    // reporting by `store-heartbeat` to PD, we should report it here manually as
                    // a FAKE `store-heartbeat`.
                    if slow_score_tick_result.should_force_report_slow_store
                        && self.is_store_heartbeat_delayed()
                    {
                        self.handle_fake_store_heartbeat();
                    }
                    LatencyInspector::new(
                        id,
                        Box::new(move |id, duration| {
                            STORE_INSPECT_DURATION_HISTOGRAM
                                .with_label_values(&["store_wait"])
                                .observe(tikv_util::time::duration_to_sec(
                                    duration.store_wait_duration.unwrap_or_default(),
                                ));
                            STORE_INSPECT_DURATION_HISTOGRAM
                                .with_label_values(&["store_commit"])
                                .observe(tikv_util::time::duration_to_sec(
                                    duration.store_commit_duration.unwrap_or_default(),
                                ));

                            STORE_INSPECT_DURATION_HISTOGRAM
                                .with_label_values(&["all"])
                                .observe(tikv_util::time::duration_to_sec(duration.sum()));
                            if let Err(e) = scheduler.schedule(Task::UpdateSlowScore {
                                id,
                                factor,
                                duration,
                            }) {
                                warn!("schedule pd task failed"; "err" => ?e);
                            }
                        }),
                    )
                }
                InspectFactor::KvDisk => LatencyInspector::new(
                    id,
                    Box::new(move |id, duration| {
                        STORE_INSPECT_DURATION_HISTOGRAM
                            .with_label_values(&["apply_wait"])
                            .observe(tikv_util::time::duration_to_sec(
                                duration.apply_wait_duration.unwrap_or_default(),
                            ));
                        STORE_INSPECT_DURATION_HISTOGRAM
                            .with_label_values(&["apply_process"])
                            .observe(tikv_util::time::duration_to_sec(
                                duration.apply_process_duration.unwrap_or_default(),
                            ));
                        if let Err(e) = scheduler.schedule(Task::UpdateSlowScore {
                            id,
                            factor,
                            duration,
                        }) {
                            warn!("schedule pd task failed"; "err" => ?e);
                        }
                    }),
                ),
                InspectFactor::Network => {
                    let network_durations = self.health_reporter.record_network_duration(id);

                    for (store_id, network_duration) in &network_durations {
                        STORE_INSPECT_NETWORK_DURATION_HISTOGRAM
                            .with_label_values(&[&store_id.to_string()])
                            .observe(tikv_util::time::duration_to_sec(*network_duration));
                    }
                    return;
                }
            }
        };
        let msg = StoreMsg::LatencyInspect {
            factor,
            send_time: TiInstant::now(),
            inspector,
        };
        if let Err(e) = self.router.send_control(msg) {
            warn!("pd worker send latency inspector failed"; "err" => ?e);
        }
    }
}

fn calculate_region_cpu_records(
    store_id: u64,
    records: Arc<RawRecords>,
    region_cpu_records: &mut HashMap<u64, u32>,
) {
    for (tag, record) in &records.records {
        let record_store_id = tag.store_id;
        if record_store_id != store_id {
            continue;
        }
        // Reporting a region heartbeat later will clear the corresponding record.
        *region_cpu_records.entry(tag.region_id).or_insert(0) += record.cpu_time;
    }
}

impl<EK, ER, T> Runnable for Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient,
{
    type Task = Task<EK>;

    fn run(&mut self, task: Task<EK>) {
        debug!("executing task"; "task" => %task);

        if !self.is_hb_receiver_scheduled {
            self.schedule_heartbeat_receiver();
        }

        match task {
            // AskSplit has deprecated, use AskBatchSplit
            Task::AskSplit {
                region,
                split_key,
                peer,
                right_derive,
                share_source_region_size,
                callback,
            } => self.handle_ask_split(
                region,
                split_key,
                peer,
                right_derive,
                share_source_region_size,
                callback,
                String::from("ask_split"),
            ),
            Task::AskBatchSplit {
                region,
                split_keys,
                peer,
                right_derive,
                share_source_region_size,
                callback,
            } => Self::handle_ask_batch_split(
                self.router.clone(),
                self.scheduler.clone(),
                self.pd_client.clone(),
                region,
                split_keys,
                peer,
                right_derive,
                share_source_region_size,
                callback,
                String::from("batch_split"),
                self.remote.clone(),
            ),
            Task::AutoSplit { split_infos } => {
                let pd_client = self.pd_client.clone();
                let router = self.router.clone();
                let scheduler = self.scheduler.clone();
                let remote = self.remote.clone();

                let f = async move {
                    for split_info in split_infos {
                        let Ok(Some(region)) =
                            pd_client.get_region_by_id(split_info.region_id).await
                        else {
                            continue;
                        };
                        // Try to split the region with the given split key.
                        if let Some(split_key) = split_info.split_key {
                            Self::handle_ask_batch_split(
                                router.clone(),
                                scheduler.clone(),
                                pd_client.clone(),
                                region,
                                vec![split_key],
                                split_info.peer,
                                true,
                                false,
                                Callback::None,
                                String::from("auto_split"),
                                remote.clone(),
                            );
                        // Try to split the region on half within the given key
                        // range if there is no `split_key` been given.
                        } else if split_info.start_key.is_some() && split_info.end_key.is_some() {
                            let start_key = split_info.start_key.unwrap();
                            let end_key = split_info.end_key.unwrap();
                            let region_id = region.get_id();
                            let msg = Box::new(CasualMessage::HalfSplitRegion {
                                region_epoch: region.get_region_epoch().clone(),
                                start_key: Some(start_key.clone()),
                                end_key: Some(end_key.clone()),
                                policy: pdpb::CheckPolicy::Scan,
                                source: "auto_split",
                                cb: Callback::None,
                            });
                            if let Err(e) = router.send(region_id, PeerMsg::CasualMessage(msg)) {
                                warn!("send auto half split request failed";
                                    "region_id" => region_id,
                                    "start_key" => log_wrappers::Value::key(&start_key),
                                    "end_key" => log_wrappers::Value::key(&end_key),
                                    "err" => ?e,
                                );
                            }
                        }
                    }
                };
                self.remote.spawn(f);
            }

            Task::Heartbeat(hb_task) => {
                // HACK! In order to keep the compatible of protos, we use 0 to identify
                // the size uninitialized regions, and use 1 to identify the empty regions.
                //
                // See tikv/tikv#11114 for details.
                let approximate_size = match hb_task.approximate_size {
                    Some(0) => 1,
                    Some(v) => v,
                    None => 0, // size uninitialized
                };
                let approximate_keys = hb_task.approximate_keys.unwrap_or_default();
                let (
                    read_bytes_delta,
                    read_keys_delta,
                    written_bytes_delta,
                    written_keys_delta,
                    last_report_ts,
                    query_stats,
                    cop_detail,
                    cpu_usage,
                ) = {
                    let region_id = hb_task.region.get_id();
                    let mut region_peers = self.region_peers.write().unwrap();
                    let peer_stat = region_peers.entry(region_id).or_default();
                    peer_stat.approximate_size = approximate_size;
                    peer_stat.approximate_keys = approximate_keys;

                    let read_bytes_delta =
                        peer_stat.read_bytes - peer_stat.last_region_report_read_bytes;
                    let read_keys_delta =
                        peer_stat.read_keys - peer_stat.last_region_report_read_keys;
                    let written_bytes_delta =
                        hb_task.written_bytes - peer_stat.last_region_report_written_bytes;
                    let written_keys_delta =
                        hb_task.written_keys - peer_stat.last_region_report_written_keys;
                    let query_stats = peer_stat
                        .query_stats
                        .sub_query_stats(&peer_stat.last_region_report_query_stats);
                    let cop_detail = peer_stat
                        .cop_detail
                        .sub(&peer_stat.last_region_report_cop_detail);
                    let mut last_report_ts = peer_stat.last_region_report_ts;
                    peer_stat.last_region_report_written_bytes = hb_task.written_bytes;
                    peer_stat.last_region_report_written_keys = hb_task.written_keys;
                    peer_stat.last_region_report_read_bytes = peer_stat.read_bytes;
                    peer_stat.last_region_report_read_keys = peer_stat.read_keys;
                    peer_stat.last_region_report_query_stats = peer_stat.query_stats.clone();
                    peer_stat.last_region_report_cop_detail = peer_stat.cop_detail.clone();
                    let unix_secs_now = UnixSecs::now();
                    peer_stat.last_region_report_ts = unix_secs_now;

                    if last_report_ts.is_zero() {
                        last_report_ts = self.start_ts;
                    }
                    // Calculate the CPU usage since the last region heartbeat.
                    let cpu_usage = {
                        // Take out the region CPU record.
                        let cpu_time_duration = Duration::from_millis(
                            self.region_cpu_records.remove(&region_id).unwrap_or(0) as u64,
                        );
                        let interval_second =
                            unix_secs_now.into_inner() - last_report_ts.into_inner();
                        // Keep consistent with the calculation of cpu_usages in a store heartbeat.
                        // See components/tikv_util/src/metrics/threads_linux.rs for more details.
                        if interval_second > 0 {
                            ((cpu_time_duration.as_secs_f64() * 100.0) / interval_second as f64)
                                as u64
                        } else {
                            0
                        }
                    };
                    (
                        read_bytes_delta,
                        read_keys_delta,
                        written_bytes_delta,
                        written_keys_delta,
                        last_report_ts,
                        query_stats.0,
                        cop_detail,
                        cpu_usage,
                    )
                };
                self.handle_heartbeat(
                    hb_task.term,
                    hb_task.region,
                    hb_task.peer,
                    RegionStat {
                        down_peers: hb_task.down_peers,
                        pending_peers: hb_task.pending_peers,
                        written_bytes: written_bytes_delta,
                        written_keys: written_keys_delta,
                        read_bytes: read_bytes_delta,
                        read_keys: read_keys_delta,
                        query_stats,
                        cop_detail,
                        approximate_size,
                        approximate_keys,
                        last_report_ts,
                        cpu_usage,
                    },
                    hb_task.replication_status,
                )
            }
            Task::StoreHeartbeat {
                stats,
                report,
                dr_autosync_status,
            } => self.handle_store_heartbeat(stats, false, report, dr_autosync_status),
            Task::ReportBatchSplit { regions } => self.handle_report_batch_split(regions),
            Task::ValidatePeer { region, peer } => self.handle_validate_peer(region, peer),
            Task::ReadStats { read_stats } => self.handle_read_stats(read_stats),
            Task::WriteStats { write_stats } => self.handle_write_stats(write_stats),
            Task::DestroyPeer { region_id } => self.handle_destroy_peer(region_id),
            Task::StoreInfos {
                cpu_usages,
                read_io_rates,
                write_io_rates,
            } => self.handle_store_infos(cpu_usages, read_io_rates, write_io_rates),
            Task::UpdateMaxTimestamp {
                region_id,
                initial_status,
                txn_ext,
            } => self.handle_update_max_timestamp(region_id, initial_status, txn_ext),
            Task::QueryRegionLeader { region_id } => self.handle_query_region_leader(region_id),
            Task::UpdateSlowScore {
                id,
                factor,
                duration,
            } => {
                self.health_reporter.record_duration(
                    id,
                    factor,
                    duration,
                    !self.store_stat.maybe_busy(),
                );
            }
            Task::RegionCpuRecords(records) => self.handle_region_cpu_records(records),
            Task::ReportMinResolvedTs {
                store_id,
                min_resolved_ts,
            } => self.handle_report_min_resolved_ts(store_id, min_resolved_ts),
            Task::ReportBuckets(buckets) => {
                self.handle_report_region_buckets(buckets);
            }
            Task::ControlGrpcServer(event) => {
                self.handle_control_grpc_server(event);
            }
            Task::InspectLatency { factor } => {
                self.handle_inspect_latency(factor);
            }
        };
    }

    fn shutdown(&mut self) {
        self.stats_monitor.stop();
    }
}

fn new_change_peer_request(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
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

fn new_split_region_request(
    split_key: Vec<u8>,
    new_region_id: u64,
    peer_ids: Vec<u64>,
    right_derive: bool,
    share_source_region_size: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_region_id(new_region_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req.mut_split().set_right_derive(right_derive);
    req.mut_split()
        .set_share_source_region_size(share_source_region_size);
    req
}

fn new_batch_split_region_request(
    split_keys: Vec<Vec<u8>>,
    ids: Vec<pdpb::SplitId>,
    right_derive: bool,
    share_source_region_size: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::BatchSplit);
    req.mut_splits().set_right_derive(right_derive);
    req.mut_splits()
        .set_share_source_region_size(share_source_region_size);
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

fn new_transfer_leader_request(peer: metapb::Peer, peers: Vec<metapb::Peer>) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
    req.mut_transfer_leader().set_peers(peers.into());
    req
}

fn new_merge_request(merge: pdpb::Merge) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::PrepareMerge);
    req.mut_prepare_merge()
        .set_target(merge.get_target().to_owned());
    req
}

fn new_batch_switch_witness(switches: Vec<pdpb::SwitchWitness>) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::BatchSwitchWitness);
    let switch_reqs = switches
        .into_iter()
        .map(|s| {
            let mut sw = SwitchWitnessRequest::default();
            sw.set_peer_id(s.get_peer_id());
            sw.set_is_witness(s.get_is_witness());
            sw
        })
        .collect();
    let mut sw = BatchSwitchWitnessRequest::default();
    sw.set_switch_witnesses(switch_reqs);
    req.set_switch_witnesses(sw);
    req
}

fn send_admin_request<EK, ER>(
    router: &RaftRouter<EK, ER>,
    region_id: u64,
    epoch: metapb::RegionEpoch,
    peer: metapb::Peer,
    request: AdminRequest,
    callback: Callback<EK::Snapshot>,
    extra_opts: RaftCmdExtraOpts,
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

    let cmd = RaftCommand::new_ext(req, callback, extra_opts);
    if let Err(e) = router.send_raft_command(cmd) {
        warn!(
            "send request failed";
            "region_id" => region_id, "cmd_type" => ?cmd_type, "err" => ?e,
        );
    }
}

/// Sends a raft message to destroy the specified stale Peer
fn send_destroy_peer_message<EK, ER>(
    router: &RaftRouter<EK, ER>,
    local_region: metapb::Region,
    peer: metapb::Peer,
    pd_region: metapb::Region,
) where
    EK: KvEngine,
    ER: RaftEngine,
{
    let mut message = RaftMessage::default();
    message.set_region_id(local_region.get_id());
    message.set_from_peer(peer.clone());
    message.set_to_peer(peer);
    message.set_region_epoch(pd_region.get_region_epoch().clone());
    message.set_is_tombstone(true);
    if let Err(e) = router.send_raft_message(message) {
        error!(
            "send gc peer request failed";
            "region_id" => local_region.get_id(),
            "err" => ?e
        )
    }
}

fn collect_report_read_peer_stats(
    capacity: usize,
    mut report_read_stats: HashMap<u64, pdpb::PeerStat>,
    mut stats: pdpb::StoreStats,
) -> pdpb::StoreStats {
    if report_read_stats.len() < capacity * 3 {
        for (_, read_stat) in report_read_stats {
            stats.peer_stats.push(read_stat);
        }
        return stats;
    }
    let mut keys_topn_report = TopN::new(capacity);
    let mut bytes_topn_report = TopN::new(capacity);
    let mut stats_topn_report = TopN::new(capacity);
    for read_stat in report_read_stats.values() {
        let mut cmp_stat = PeerCmpReadStat::default();
        cmp_stat.region_id = read_stat.region_id;
        let mut key_cmp_stat = cmp_stat.clone();
        key_cmp_stat.report_stat = read_stat.read_keys;
        keys_topn_report.push(key_cmp_stat);
        let mut byte_cmp_stat = cmp_stat.clone();
        byte_cmp_stat.report_stat = read_stat.read_bytes;
        bytes_topn_report.push(byte_cmp_stat);
        let mut query_cmp_stat = cmp_stat.clone();
        query_cmp_stat.report_stat = get_read_query_num(read_stat.get_query_stats());
        stats_topn_report.push(query_cmp_stat);
    }

    for x in keys_topn_report {
        if let Some(report_stat) = report_read_stats.remove(&x.region_id) {
            stats.peer_stats.push(report_stat);
        }
    }

    for x in bytes_topn_report {
        if let Some(report_stat) = report_read_stats.remove(&x.region_id) {
            stats.peer_stats.push(report_stat);
        }
    }

    for x in stats_topn_report {
        if let Some(report_stat) = report_read_stats.remove(&x.region_id) {
            stats.peer_stats.push(report_stat);
        }
    }
    stats
}

fn collect_engine_size<EK: KvEngine>(coprocessor_host: &CoprocessorHost<EK>) -> (u64, u64, u64) {
    if let Some(engine_size) = coprocessor_host.on_compute_engine_size() {
        (engine_size.capacity, engine_size.used, engine_size.avail)
    } else {
        (
            disk::get_disk_capacity(),
            disk::get_disk_used_size(),
            disk::get_disk_available_size(),
        )
    }
}

fn get_read_query_num(stat: &pdpb::QueryStats) -> u64 {
    stat.get_get() + stat.get_coprocessor() + stat.get_scan()
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use kvproto::{kvrpcpb, pdpb::QueryKind};
    use pd_client::{BucketMeta, new_bucket_stats};
    use tikv_util::worker::LazyWorker;

    use super::*;
    use crate::store::util::build_key_range;

    const DEFAULT_TEST_STORE_ID: u64 = 1;

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn test_collect_stats() {
        use std::{sync::Mutex, time::Instant};

        use engine_test::kv::KvTestEngine;

        struct RunnerTest {
            store_stat: Arc<Mutex<StoreStat>>,
            stats_monitor: StatsMonitor<WrappedScheduler<KvTestEngine>>,
        }

        impl RunnerTest {
            fn new(
                interval: u64,
                scheduler: Scheduler<Task<KvTestEngine>>,
                store_stat: Arc<Mutex<StoreStat>>,
            ) -> RunnerTest {
                let mut stats_monitor = StatsMonitor::new(
                    Duration::from_secs(interval),
                    Duration::from_secs(interval),
                    Duration::default(),
                    Duration::default(),
                    WrappedScheduler(scheduler),
                );
                if let Err(e) = stats_monitor.start(
                    AutoSplitController::default(),
                    CollectorRegHandle::new_for_test(),
                ) {
                    error!("failed to start stats collector, error = {:?}", e);
                }

                RunnerTest {
                    store_stat,
                    stats_monitor,
                }
            }

            fn handle_store_infos(
                &mut self,
                cpu_usages: RecordPairVec,
                read_io_rates: RecordPairVec,
                write_io_rates: RecordPairVec,
            ) {
                let mut store_stat = self.store_stat.lock().unwrap();
                store_stat.store_cpu_usages = cpu_usages;
                store_stat.store_read_io_rates = read_io_rates;
                store_stat.store_write_io_rates = write_io_rates;
            }
        }

        impl Runnable for RunnerTest {
            type Task = Task<KvTestEngine>;

            fn run(&mut self, task: Task<KvTestEngine>) {
                if let Task::StoreInfos {
                    cpu_usages,
                    read_io_rates,
                    write_io_rates,
                } = task
                {
                    self.handle_store_infos(cpu_usages, read_io_rates, write_io_rates)
                };
            }

            fn shutdown(&mut self) {
                self.stats_monitor.stop();
            }
        }

        fn sum_record_pairs(pairs: &[pdpb::RecordPair]) -> u64 {
            let mut sum = 0;
            for record in pairs.iter() {
                sum += record.get_value();
            }
            sum
        }

        let mut pd_worker = LazyWorker::new("test-pd-worker");
        let store_stat = Arc::new(Mutex::new(StoreStat::default()));
        let runner = RunnerTest::new(1, pd_worker.scheduler(), Arc::clone(&store_stat));
        assert!(pd_worker.start(runner));

        let start = Instant::now();
        loop {
            if (Instant::now() - start).as_secs() > 2 {
                break;
            }
        }

        let total_cpu_usages = sum_record_pairs(&store_stat.lock().unwrap().store_cpu_usages);
        assert!(total_cpu_usages > 90);

        pd_worker.stop();
    }

    #[test]
    fn test_collect_report_peers() {
        let mut report_stats = HashMap::default();
        for i in 1..5 {
            let mut stat = pdpb::PeerStat::default();
            stat.set_region_id(i);
            stat.set_read_keys(i);
            stat.set_read_bytes(6 - i);
            stat.read_keys = i;
            stat.read_bytes = 6 - i;
            let mut query_stat = QueryStats::default();
            if i == 3 {
                query_stat.add_query_num(QueryKind::Get, 6);
            } else {
                query_stat.add_query_num(QueryKind::Get, 0);
            }
            stat.set_query_stats(query_stat.0);
            report_stats.insert(i, stat);
        }
        let mut store_stats = pdpb::StoreStats::default();
        store_stats = collect_report_read_peer_stats(1, report_stats, store_stats);
        assert_eq!(store_stats.peer_stats.len(), 3)
    }

    use engine_test::kv::KvTestEngine;
    use metapb::Peer;
    use resource_metering::{RawRecord, TagInfos};

    use crate::coprocessor::{BoxPdTaskObserver, Coprocessor, PdTaskObserver, StoreSizeInfo};

    #[test]
    fn test_calculate_region_cpu_records() {
        // region_id -> total_cpu_time_ms
        let mut region_cpu_records: HashMap<u64, u32> = HashMap::default();

        let region_num = 3;
        for i in 0..region_num * 10 {
            let cpu_records = Arc::new(RawRecords {
                begin_unix_time_secs: UnixSecs::now().into_inner(),
                duration: Duration::default(),
                records: {
                    let region_id = i % region_num + 1_u64;
                    let peer_id = region_id + region_num;
                    let resource_group_tag = "test-resource-group".as_bytes().to_vec();
                    let mut peer = Peer::default();
                    peer.set_id(peer_id);
                    peer.set_store_id(DEFAULT_TEST_STORE_ID);
                    let mut context = kvrpcpb::Context::default();
                    context.set_peer(peer);
                    context.set_region_id(region_id);
                    context.set_resource_group_tag(resource_group_tag);
                    let resource_tag = Arc::new(TagInfos::from_rpc_context(&context));

                    let mut records = HashMap::default();
                    records.insert(
                        resource_tag,
                        RawRecord {
                            cpu_time: 10,
                            read_keys: 0,
                            write_keys: 0,
                            network_in_bytes: 0,
                            network_out_bytes: 0,
                            logical_read_bytes: 0,
                            logical_write_bytes: 0,
                        },
                    );
                    records
                },
            });

            calculate_region_cpu_records(
                DEFAULT_TEST_STORE_ID,
                cpu_records,
                &mut region_cpu_records,
            );

            sleep(Duration::from_millis(50));
        }

        for region_id in 1..region_num + 1 {
            assert!(*region_cpu_records.get(&region_id).unwrap_or(&0) > 0)
        }
    }

    #[test]
    fn test_report_bucket_stats() {
        #[allow(clippy::type_complexity)]
        let cases: &[((Vec<&[_]>, _), (Vec<&[_]>, _), _)] = &[
            (
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![2, 2, 2, 2]),
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![1, 1, 1, 1]),
                vec![1, 1, 1, 1],
            ),
            (
                (vec![b"k1", b"k3", b"k5", b"k7", b"k9"], vec![2, 2, 2, 2]),
                (vec![b"k0", b"k6", b"k8"], vec![1, 1]),
                vec![1, 1, 0, 1],
            ),
            (
                (vec![b"k4", b"k6", b"kb"], vec![5, 5]),
                (
                    vec![b"k1", b"k3", b"k5", b"k7", b"k9", b"ka"],
                    vec![1, 1, 1, 1, 1],
                ),
                vec![3, 2],
            ),
        ];
        for (current, last, expected) in cases {
            let cur_keys = &current.0;
            let last_keys = &last.0;

            let mut cur_meta = BucketMeta::default();
            cur_meta.keys = cur_keys.iter().map(|k| k.to_vec()).collect();
            let mut cur_stats = new_bucket_stats(&cur_meta);
            cur_stats.set_read_qps(current.1.to_vec());

            let mut last_meta = BucketMeta::default();
            last_meta.keys = last_keys.iter().map(|k| k.to_vec()).collect();
            let mut last_stats = new_bucket_stats(&last_meta);
            last_stats.set_read_qps(last.1.to_vec());
            let mut bucket = ReportBucket {
                current_stat: BucketStat {
                    meta: Arc::new(cur_meta),
                    stats: cur_stats,
                    create_time: TiInstant::now(),
                },
                last_report_stat: Some(BucketStat {
                    meta: Arc::new(last_meta),
                    stats: last_stats,
                    create_time: TiInstant::now(),
                }),
                last_report_ts: UnixSecs::now(),
            };
            let report = bucket.new_report(UnixSecs::now());
            assert_eq!(report.stats.get_read_qps(), expected);
        }
    }

    #[derive(Debug, Clone, Default)]
    struct PdObserver {}

    impl Coprocessor for PdObserver {}

    impl PdTaskObserver for PdObserver {
        fn on_compute_engine_size(&self, s: &mut Option<StoreSizeInfo>) {
            let _ = s.insert(StoreSizeInfo {
                capacity: 444,
                used: 111,
                avail: 333,
            });
        }
    }

    #[test]
    fn test_pd_task_observer() {
        let mut host = CoprocessorHost::<KvTestEngine>::default();
        let obs = PdObserver::default();
        host.registry
            .register_pd_task_observer(1, BoxPdTaskObserver::new(obs));
        let (cap, used, avail) = collect_engine_size::<KvTestEngine>(&host);
        assert_eq!(cap, 444);
        assert_eq!(used, 111);
        assert_eq!(avail, 333);
    }

    #[test]
    fn test_pd_worker_send_stats_on_read_and_cpu() {
        let mut pd_worker: LazyWorker<Task<KvTestEngine>> =
            LazyWorker::new("test-pd-worker-collect-stats");
        // Set the interval long enough for mocking the channel full state.
        let interval = 600_u64;
        let mut stats_monitor = StatsMonitor::new(
            Duration::from_secs(interval),
            Duration::from_secs(interval),
            Duration::default(),
            Duration::default(),
            WrappedScheduler(pd_worker.scheduler()),
        );
        stats_monitor
            .start(
                AutoSplitController::default(),
                CollectorRegHandle::new_for_test(),
            )
            .unwrap();
        // Add some read stats and cpu stats to the stats monitor.
        {
            for _ in 0..=STATS_CHANNEL_CAPACITY_LIMIT + 10 {
                let mut read_stats = ReadStats::with_sample_num(1);
                read_stats.add_query_num(
                    1,
                    &Peer::default(),
                    build_key_range(b"a", b"b", false),
                    QueryKind::Get,
                );
                stats_monitor.maybe_send_read_stats(read_stats);
            }

            let raw_records = Arc::new(RawRecords {
                begin_unix_time_secs: UnixSecs::now().into_inner(),
                duration: Duration::default(),
                records: {
                    let mut records = HashMap::default();
                    records.insert(
                        Arc::new(TagInfos {
                            store_id: 0,
                            region_id: 1,
                            peer_id: 0,
                            key_ranges: vec![],
                            extra_attachment: Arc::new(b"a".to_vec()),
                        }),
                        RawRecord {
                            cpu_time: 111,
                            read_keys: 1,
                            write_keys: 0,
                            network_in_bytes: 0,
                            network_out_bytes: 0,
                            logical_read_bytes: 0,
                            logical_write_bytes: 0,
                        },
                    );
                    records
                },
            });
            for _ in 0..=STATS_CHANNEL_CAPACITY_LIMIT + 10 {
                stats_monitor.maybe_send_cpu_stats(&raw_records);
            }
        }

        pd_worker.stop();
    }
}
