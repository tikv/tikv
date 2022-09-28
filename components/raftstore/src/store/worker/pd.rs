// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    cmp::Ordering as CmpOrdering,
    fmt::{self, Display, Formatter},
    io, mem,
    sync::{
        atomic::Ordering,
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::{Duration, Instant},
};

use causal_ts::{CausalTsProvider, CausalTsProviderImpl};
use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use futures::{compat::Future01CompatExt, FutureExt};
use grpcio_health::{HealthService, ServingStatus};
use kvproto::{
    kvrpcpb::DiskFullOpt,
    metapb, pdpb,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, ChangePeerRequest, ChangePeerV2Request, RaftCmdRequest,
        SplitRequest,
    },
    raft_serverpb::RaftMessage,
    replication_modepb::{RegionReplicationStatus, StoreDrAutoSyncStatus},
};
use ordered_float::OrderedFloat;
use pd_client::{merge_bucket_stats, metrics::*, BucketStat, Error, PdClient, RegionStat};
use prometheus::local::LocalHistogram;
use raft::eraftpb::ConfChangeType;
use resource_metering::{Collector, CollectorGuard, CollectorRegHandle, RawRecords};
use tikv_util::{
    box_err, debug, error, info,
    metrics::ThreadInfoStatistics,
    store::QueryStats,
    sys::thread::StdThreadBuildWrapper,
    thd_name,
    time::{Instant as TiInstant, UnixSecs},
    timer::GLOBAL_TIMER_HANDLE,
    topn::TopN,
    warn,
    worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler},
};
use txn_types::TimeStamp;
use yatp::Remote;

use crate::{
    coprocessor::CoprocessorHost,
    store::{
        cmd_resp::new_error,
        metrics::*,
        peer::{UnsafeRecoveryExecutePlanSyncer, UnsafeRecoveryForceLeaderSyncer},
        transport::SignificantRouter,
        util::{is_epoch_stale, KeysInfoFormatter, LatencyInspector, RaftstoreDuration},
        worker::{
            split_controller::{SplitInfo, TOP_N},
            AutoSplitController, ReadStats, SplitConfigChange, WriteStats,
        },
        Callback, CasualMessage, Config, PeerMsg, RaftCmdExtraOpts, RaftCommand, RaftRouter,
        RegionReadProgressRegistry, SignificantMsg, SnapManager, StoreInfo, StoreMsg, TxnExt,
    },
};

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

impl<EK, ER> FlowStatsReporter for Scheduler<Task<EK, ER>>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn report_read_stats(&self, read_stats: ReadStats) {
        if let Err(e) = self.schedule(Task::ReadStats { read_stats }) {
            error!("Failed to send read flow statistics"; "err" => ?e);
        }
    }

    fn report_write_stats(&self, write_stats: WriteStats) {
        if let Err(e) = self.schedule(Task::WriteStats { write_stats }) {
            error!("Failed to send write flow statistics"; "err" => ?e);
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
}

/// Uses an asynchronous thread to tell PD something.
pub enum Task<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        callback: Callback<EK::Snapshot>,
    },
    AskBatchSplit {
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        callback: Callback<EK::Snapshot>,
    },
    AutoSplit {
        split_infos: Vec<SplitInfo>,
    },
    Heartbeat(HeartbeatTask),
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        store_info: StoreInfo<EK, ER>,
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
        duration: RaftstoreDuration,
    },
    UpdateRegionCpuCollector(bool),
    RegionCpuRecords(Arc<RawRecords>),
    ReportMinResolvedTs {
        store_id: u64,
        min_resolved_ts: u64,
    },
    ReportBuckets(BucketStat),
}

pub struct StoreStat {
    pub engine_total_bytes_read: u64,
    pub engine_total_keys_read: u64,
    pub engine_total_query_num: QueryStats,
    pub engine_last_total_bytes_read: u64,
    pub engine_last_total_keys_read: u64,
    pub engine_last_query_num: QueryStats,
    pub last_report_ts: UnixSecs,

    pub region_bytes_read: LocalHistogram,
    pub region_keys_read: LocalHistogram,
    pub region_bytes_written: LocalHistogram,
    pub region_keys_written: LocalHistogram,

    pub store_cpu_usages: RecordPairVec,
    pub store_read_io_rates: RecordPairVec,
    pub store_write_io_rates: RecordPairVec,
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
            engine_total_query_num: QueryStats::default(),
            engine_last_query_num: QueryStats::default(),

            store_cpu_usages: RecordPairVec::default(),
            store_read_io_rates: RecordPairVec::default(),
            store_write_io_rates: RecordPairVec::default(),
        }
    }
}

#[derive(Default)]
pub struct PeerStat {
    pub read_bytes: u64,
    pub read_keys: u64,
    pub query_stats: QueryStats,
    // last_region_report_attributes records the state of the last region heartbeat
    pub last_region_report_read_bytes: u64,
    pub last_region_report_read_keys: u64,
    pub last_region_report_query_stats: QueryStats,
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
pub struct ReportBucket {
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
                let mut delta = BucketStat::new(
                    self.current_stat.meta.clone(),
                    pd_client::new_bucket_stats(&self.current_stat.meta),
                );
                // Buckets may be changed, recalculate last stats according to current meta.
                merge_bucket_stats(
                    &delta.meta.keys,
                    &mut delta.stats,
                    &last.meta.keys,
                    &last.stats,
                );
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

impl<EK, ER> Display for Task<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
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
            Task::UpdateSlowScore { id, ref duration } => {
                write!(f, "compute slow score: id {}, duration {:?}", id, duration)
            }
            Task::UpdateRegionCpuCollector(is_register) => {
                if is_register {
                    return write!(f, "register region cpu collector");
                }
                write!(f, "deregister region cpu collector")
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

fn config(interval: Duration) -> Duration {
    fail_point!("mock_min_resolved_ts_interval", |_| {
        Duration::from_millis(50)
    });
    fail_point!("mock_min_resolved_ts_interval_disable", |_| {
        Duration::from_millis(0)
    });
    interval
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

struct StatsMonitor<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    scheduler: Scheduler<Task<EK, ER>>,
    handle: Option<JoinHandle<()>>,
    timer: Option<Sender<bool>>,
    read_stats_sender: Option<Sender<ReadStats>>,
    cpu_stats_sender: Option<Sender<Arc<RawRecords>>>,
    collect_store_infos_interval: Duration,
    load_base_split_check_interval: Duration,
    collect_tick_interval: Duration,
    report_min_resolved_ts_interval: Duration,
}

impl<EK, ER> StatsMonitor<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        interval: Duration,
        report_min_resolved_ts_interval: Duration,
        scheduler: Scheduler<Task<EK, ER>>,
    ) -> Self {
        StatsMonitor {
            scheduler,
            handle: None,
            timer: None,
            read_stats_sender: None,
            cpu_stats_sender: None,
            collect_store_infos_interval: interval,
            load_base_split_check_interval: cmp::min(
                DEFAULT_LOAD_BASE_SPLIT_CHECK_INTERVAL,
                interval,
            ),
            report_min_resolved_ts_interval: config(report_min_resolved_ts_interval),
            collect_tick_interval: cmp::min(default_collect_tick_interval(), interval),
        }
    }

    // Collecting thread information and obtaining qps information for auto split.
    // They run together in the same thread by taking modulo at different intervals.
    pub fn start(
        &mut self,
        mut auto_split_controller: AutoSplitController,
        region_read_progress: RegionReadProgressRegistry,
        store_id: u64,
    ) -> Result<(), io::Error> {
        if self.collect_tick_interval < default_collect_tick_interval()
            || self.collect_store_infos_interval < self.collect_tick_interval
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
        let report_min_resolved_ts_interval = self
            .report_min_resolved_ts_interval
            .div_duration_f64(tick_interval) as u64;

        let (timer_tx, timer_rx) = mpsc::channel();
        self.timer = Some(timer_tx);

        let (read_stats_sender, read_stats_receiver) = mpsc::channel();
        self.read_stats_sender = Some(read_stats_sender);

        let (cpu_stats_sender, cpu_stats_receiver) = mpsc::channel();
        self.cpu_stats_sender = Some(cpu_stats_sender);

        let scheduler = self.scheduler.clone();
        let props = tikv_util::thread_group::current_properties();

        fn is_enable_tick(timer_cnt: u64, interval: u64) -> bool {
            interval != 0 && timer_cnt % interval == 0
        }
        let h = Builder::new()
            .name(thd_name!("stats-monitor"))
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                tikv_alloc::add_thread_memory_accessor();
                // Create different `ThreadInfoStatistics` for different purposes to
                // make sure the record won't be disturbed.
                let mut collect_store_infos_thread_stats = ThreadInfoStatistics::new();
                let mut load_base_split_thread_stats = ThreadInfoStatistics::new();
                while let Err(mpsc::RecvTimeoutError::Timeout) =
                    timer_rx.recv_timeout(tick_interval)
                {
                    if is_enable_tick(timer_cnt, collect_store_infos_interval) {
                        StatsMonitor::collect_store_infos(
                            &mut collect_store_infos_thread_stats,
                            &scheduler,
                        );
                    }
                    if is_enable_tick(timer_cnt, load_base_split_check_interval) {
                        StatsMonitor::load_base_split(
                            &mut auto_split_controller,
                            &read_stats_receiver,
                            &cpu_stats_receiver,
                            &mut load_base_split_thread_stats,
                            &scheduler,
                        );
                    }
                    if is_enable_tick(timer_cnt, report_min_resolved_ts_interval) {
                        StatsMonitor::report_min_resolved_ts(
                            &region_read_progress,
                            store_id,
                            &scheduler,
                        );
                    }
                    timer_cnt += 1;
                }
                tikv_alloc::remove_thread_memory_accessor();
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn collect_store_infos(
        thread_stats: &mut ThreadInfoStatistics,
        scheduler: &Scheduler<Task<EK, ER>>,
    ) {
        thread_stats.record();
        let cpu_usages = convert_record_pairs(thread_stats.get_cpu_usages());
        let read_io_rates = convert_record_pairs(thread_stats.get_read_io_rates());
        let write_io_rates = convert_record_pairs(thread_stats.get_write_io_rates());

        let task = Task::StoreInfos {
            cpu_usages,
            read_io_rates,
            write_io_rates,
        };
        if let Err(e) = scheduler.schedule(task) {
            error!(
                "failed to send store infos to pd worker";
                "err" => ?e,
            );
        }
    }

    pub fn load_base_split(
        auto_split_controller: &mut AutoSplitController,
        read_stats_receiver: &Receiver<ReadStats>,
        cpu_stats_receiver: &Receiver<Arc<RawRecords>>,
        thread_stats: &mut ThreadInfoStatistics,
        scheduler: &Scheduler<Task<EK, ER>>,
    ) {
        let start_time = TiInstant::now();
        match auto_split_controller.refresh_and_check_cfg() {
            SplitConfigChange::UpdateRegionCpuCollector(is_register) => {
                if let Err(e) = scheduler.schedule(Task::UpdateRegionCpuCollector(is_register)) {
                    error!(
                        "failed to register or deregister the region cpu collector";
                        "is_register" => is_register,
                        "err" => ?e,
                    );
                }
            }
            SplitConfigChange::Noop => {}
        }
        let mut read_stats_vec = vec![];
        while let Ok(read_stats) = read_stats_receiver.try_recv() {
            read_stats_vec.push(read_stats);
        }
        let mut cpu_stats_vec = vec![];
        while let Ok(cpu_stats) = cpu_stats_receiver.try_recv() {
            cpu_stats_vec.push(cpu_stats);
        }
        thread_stats.record();
        let (top_qps, split_infos) =
            auto_split_controller.flush(read_stats_vec, cpu_stats_vec, thread_stats);
        auto_split_controller.clear();
        let task = Task::AutoSplit { split_infos };
        if let Err(e) = scheduler.schedule(task) {
            error!(
                "failed to send split infos to pd worker";
                "err" => ?e,
            );
        }
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

    pub fn report_min_resolved_ts(
        region_read_progress: &RegionReadProgressRegistry,
        store_id: u64,
        scheduler: &Scheduler<Task<EK, ER>>,
    ) {
        let task = Task::ReportMinResolvedTs {
            store_id,
            min_resolved_ts: region_read_progress.get_min_resolved_ts(),
        };
        if let Err(e) = scheduler.schedule(task) {
            error!(
                "failed to send min resolved ts to pd worker";
                "err" => ?e,
            );
        }
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

    #[inline(always)]
    fn get_read_stats_sender(&self) -> &Option<Sender<ReadStats>> {
        &self.read_stats_sender
    }

    #[inline(always)]
    fn get_cpu_stats_sender(&self) -> &Option<Sender<Arc<RawRecords>>> {
        &self.cpu_stats_sender
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

// Slow score is a value that represents the speed of a store and ranges in [1,
// 100]. It is maintained in the AIMD way.
// If there are some inspecting requests timeout during a round, by default the
// score will be increased at most 1x when above 10% inspecting requests
// timeout. If there is not any timeout inspecting requests, the score will go
// back to 1 in at least 5min.
struct SlowScore {
    value: OrderedFloat<f64>,
    last_record_time: Instant,
    last_update_time: Instant,

    timeout_requests: usize,
    total_requests: usize,

    inspect_interval: Duration,
    // The maximal tolerated timeout ratio.
    ratio_thresh: OrderedFloat<f64>,
    // Minimal time that the score could be decreased from 100 to 1.
    min_ttr: Duration,

    // After how many ticks the value need to be updated.
    round_ticks: u64,
    // Identify every ticks.
    last_tick_id: u64,
    // If the last tick does not finished, it would be recorded as a timeout.
    last_tick_finished: bool,
}

impl SlowScore {
    fn new(inspect_interval: Duration) -> SlowScore {
        SlowScore {
            value: OrderedFloat(1.0),

            timeout_requests: 0,
            total_requests: 0,

            inspect_interval,
            ratio_thresh: OrderedFloat(0.1),
            min_ttr: Duration::from_secs(5 * 60),
            last_record_time: Instant::now(),
            last_update_time: Instant::now(),
            round_ticks: 30,
            last_tick_id: 0,
            last_tick_finished: true,
        }
    }

    fn record(&mut self, id: u64, duration: Duration) {
        self.last_record_time = Instant::now();
        if id != self.last_tick_id {
            return;
        }
        self.last_tick_finished = true;
        self.total_requests += 1;
        if duration >= self.inspect_interval {
            self.timeout_requests += 1;
        }
    }

    fn record_timeout(&mut self) {
        self.last_tick_finished = true;
        self.total_requests += 1;
        self.timeout_requests += 1;
    }

    fn update(&mut self) -> f64 {
        let elapsed = self.last_update_time.elapsed();
        self.update_impl(elapsed).into()
    }

    fn get(&self) -> f64 {
        self.value.into()
    }

    // Update the score in a AIMD way.
    fn update_impl(&mut self, elapsed: Duration) -> OrderedFloat<f64> {
        if self.timeout_requests == 0 {
            let desc = 100.0 * (elapsed.as_millis() as f64 / self.min_ttr.as_millis() as f64);
            if OrderedFloat(desc) > self.value - OrderedFloat(1.0) {
                self.value = 1.0.into();
            } else {
                self.value -= desc;
            }
        } else {
            let timeout_ratio = self.timeout_requests as f64 / self.total_requests as f64;
            let near_thresh =
                cmp::min(OrderedFloat(timeout_ratio), self.ratio_thresh) / self.ratio_thresh;
            let value = self.value * (OrderedFloat(1.0) + near_thresh);
            self.value = cmp::min(OrderedFloat(100.0), value);
        }

        self.total_requests = 0;
        self.timeout_requests = 0;
        self.last_update_time = Instant::now();
        self.value
    }
}

// RegionCpuMeteringCollector is used to collect the region-related CPU info.
struct RegionCpuMeteringCollector<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    scheduler: Scheduler<Task<EK, ER>>,
}

impl<EK, ER> RegionCpuMeteringCollector<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(scheduler: Scheduler<Task<EK, ER>>) -> RegionCpuMeteringCollector<EK, ER> {
        RegionCpuMeteringCollector { scheduler }
    }
}

impl<EK, ER> Collector for RegionCpuMeteringCollector<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn collect(&self, records: Arc<RawRecords>) {
        self.scheduler
            .schedule(Task::RegionCpuRecords(records))
            .ok();
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
    router: RaftRouter<EK, ER>,
    region_peers: HashMap<u64, PeerStat>,
    region_buckets: HashMap<u64, ReportBucket>,
    store_stat: StoreStat,
    is_hb_receiver_scheduled: bool,
    // Records the boot time.
    start_ts: UnixSecs,

    // use for Runner inner handle function to send Task to itself
    // actually it is the sender connected to Runner's Worker which
    // calls Runner's run() on Task received.
    scheduler: Scheduler<Task<EK, ER>>,
    stats_monitor: StatsMonitor<EK, ER>,

    collector_reg_handle: CollectorRegHandle,
    region_cpu_records_collector: Option<CollectorGuard>,
    // region_id -> total_cpu_time_ms (since last region heartbeat)
    region_cpu_records: HashMap<u64, u32>,

    concurrency_manager: ConcurrencyManager,
    snap_mgr: SnapManager,
    remote: Remote<yatp::task::future::TaskCell>,
    slow_score: SlowScore,

    // The health status of the store is updated by the slow score mechanism.
    health_service: Option<HealthService>,
    curr_health_status: ServingStatus,
    coprocessor_host: CoprocessorHost<EK>,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    const INTERVAL_DIVISOR: u32 = 2;

    pub fn new(
        cfg: &Config,
        store_id: u64,
        pd_client: Arc<T>,
        router: RaftRouter<EK, ER>,
        scheduler: Scheduler<Task<EK, ER>>,
        store_heartbeat_interval: Duration,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
        snap_mgr: SnapManager,
        remote: Remote<yatp::task::future::TaskCell>,
        collector_reg_handle: CollectorRegHandle,
        region_read_progress: RegionReadProgressRegistry,
        health_service: Option<HealthService>,
        coprocessor_host: CoprocessorHost<EK>,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
    ) -> Runner<EK, ER, T> {
        // Register the region CPU records collector.
        let mut region_cpu_records_collector = None;
        if auto_split_controller
            .cfg
            .region_cpu_overload_threshold_ratio
            > 0.0
        {
            region_cpu_records_collector = Some(collector_reg_handle.register(
                Box::new(RegionCpuMeteringCollector::new(scheduler.clone())),
                false,
            ));
        }
        let interval = store_heartbeat_interval / Self::INTERVAL_DIVISOR;
        let mut stats_monitor = StatsMonitor::new(
            interval,
            cfg.report_min_resolved_ts_interval.0,
            scheduler.clone(),
        );
        if let Err(e) = stats_monitor.start(auto_split_controller, region_read_progress, store_id) {
            error!("failed to start stats collector, error = {:?}", e);
        }

        Runner {
            store_id,
            pd_client,
            router,
            is_hb_receiver_scheduled: false,
            region_peers: HashMap::default(),
            region_buckets: HashMap::default(),
            store_stat: StoreStat::default(),
            start_ts: UnixSecs::now(),
            scheduler,
            stats_monitor,
            collector_reg_handle,
            region_cpu_records_collector,
            region_cpu_records: HashMap::default(),
            concurrency_manager,
            snap_mgr,
            remote,
            slow_score: SlowScore::new(cfg.inspect_interval.0),
            health_service,
            curr_health_status: ServingStatus::Serving,
            coprocessor_host,
            causal_ts_provider,
        }
    }

    // Deprecate
    fn handle_ask_split(
        &self,
        mut region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
        right_derive: bool,
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

    fn handle_update_region_cpu_collector(&mut self, is_register: bool) {
        // If it's a deregister task, just take and drop the original collector.
        if !is_register {
            self.region_cpu_records_collector.take();
            return;
        }
        if self.region_cpu_records_collector.is_some() {
            return;
        }
        self.region_cpu_records_collector = Some(self.collector_reg_handle.register(
            Box::new(RegionCpuMeteringCollector::new(self.scheduler.clone())),
            false,
        ));
    }

    // Note: The parameter doesn't contain `self` because this function may
    // be called in an asynchronous context.
    fn handle_ask_batch_split(
        router: RaftRouter<EK, ER>,
        scheduler: Scheduler<Task<EK, ER>>,
        pd_client: Arc<T>,
        mut region: metapb::Region,
        mut split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
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
        store_info: StoreInfo<EK, ER>,
        store_report: Option<pdpb::StoreReport>,
        dr_autosync_status: Option<StoreDrAutoSyncStatus>,
    ) {
        let mut report_peers = HashMap::default();
        for (region_id, region_peer) in &mut self.region_peers {
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

        stats = collect_report_read_peer_stats(HOTSPOT_REPORT_CAPACITY, report_peers, stats);
        let (capacity, used_size, available) = match collect_engine_size(
            &self.coprocessor_host,
            Some(&store_info),
            self.snap_mgr.get_total_snap_size().unwrap(),
        ) {
            Some((capacity, used_size, available)) => (capacity, used_size, available),
            None => return,
        };

        stats.set_capacity(capacity);
        stats.set_used_size(used_size);

        if available == 0 {
            warn!("no available space");
        }
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
        self.store_stat.last_report_ts = UnixSecs::now();
        self.store_stat.region_bytes_written.flush();
        self.store_stat.region_keys_written.flush();
        self.store_stat.region_bytes_read.flush();
        self.store_stat.region_keys_read.flush();

        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["capacity"])
            .set(capacity as i64);
        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["available"])
            .set(available as i64);
        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["used"])
            .set(used_size as i64);

        let slow_score = self.slow_score.get();
        stats.set_slow_score(slow_score as u64);

        let router = self.router.clone();
        let resp = self
            .pd_client
            .store_heartbeat(stats, store_report, dr_autosync_status);
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    if let Some(status) = resp.replication_status.take() {
                        let _ = router.send_control(StoreMsg::UpdateReplicationMode(status));
                    }
                    if let Some(mut plan) = resp.recovery_plan.take() {
                        info!("Unsafe recovery, received a recovery plan");
                        if plan.has_force_leader() {
                            let mut failed_stores = HashSet::default();
                            for failed_store in plan.get_force_leader().get_failed_stores() {
                                failed_stores.insert(*failed_store);
                            }
                            let syncer = UnsafeRecoveryForceLeaderSyncer::new(
                                plan.get_step(),
                                router.clone(),
                            );
                            for region in plan.get_force_leader().get_enter_force_leaders() {
                                if let Err(e) = router.significant_send(
                                    *region,
                                    SignificantMsg::EnterForceLeaderState {
                                        syncer: syncer.clone(),
                                        failed_stores: failed_stores.clone(),
                                    },
                                ) {
                                    error!("fail to send force leader message for recovery"; "err" => ?e);
                                }
                            }
                        } else {
                            let syncer = UnsafeRecoveryExecutePlanSyncer::new(
                                plan.get_step(),
                                router.clone(),
                            );
                            for create in plan.take_creates().into_iter() {
                                if let Err(e) =
                                    router.send_control(StoreMsg::UnsafeRecoveryCreatePeer {
                                        syncer: syncer.clone(),
                                        create,
                                    })
                                {
                                    error!("fail to send create peer message for recovery"; "err" => ?e);
                                }
                            }
                            for delete in plan.take_tombstones().into_iter() {
                                if let Err(e) = router.significant_send(
                                    delete,
                                    SignificantMsg::UnsafeRecoveryDestroy(syncer.clone()),
                                ) {
                                    error!("fail to send delete peer message for recovery"; "err" => ?e);
                                }
                            }
                            for mut demote in plan.take_demotes().into_iter() {
                                if let Err(e) = router.significant_send(
                                    demote.get_region_id(),
                                    SignificantMsg::UnsafeRecoveryDemoteFailedVoters {
                                        syncer: syncer.clone(),
                                        failed_voters: demote.take_failed_voters().into_vec(),
                                    },
                                ) {
                                    error!("fail to send update peer list message for recovery"; "err" => ?e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("store heartbeat failed"; "err" => ?e);
                }
            }
        };
        self.remote.spawn(f);
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
                    // splitted Region has not yet reported to PD.
                    // TODO: handle merge
                }
                Err(e) => {
                    error!("get region failed"; "err" => ?e);
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
                    if let Err(e) = router.send(region_id, PeerMsg::CasualMessage(msg)) {
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
            let peer_stat = self
                .region_peers
                .entry(*region_id)
                .or_insert_with(PeerStat::default);
            peer_stat.read_bytes += region_info.flow.read_bytes as u64;
            peer_stat.read_keys += region_info.flow.read_keys as u64;
            self.store_stat.engine_total_bytes_read += region_info.flow.read_bytes as u64;
            self.store_stat.engine_total_keys_read += region_info.flow.read_keys as u64;
            peer_stat
                .query_stats
                .add_query_stats(&region_info.query_stats.0);
            self.store_stat
                .engine_total_query_num
                .add_query_stats(&region_info.query_stats.0);
        }
        for (_, region_buckets) in mem::take(&mut read_stats.region_buckets) {
            self.merge_buckets(region_buckets);
        }
        if !read_stats.region_infos.is_empty() {
            if let Some(sender) = self.stats_monitor.get_read_stats_sender() {
                if sender.send(read_stats).is_err() {
                    warn!("send read_stats failed, are we shutting down?")
                }
            }
        }
    }

    fn handle_write_stats(&mut self, mut write_stats: WriteStats) {
        for (region_id, region_info) in write_stats.region_infos.iter_mut() {
            let peer_stat = self
                .region_peers
                .entry(*region_id)
                .or_insert_with(PeerStat::default);
            peer_stat.query_stats.add_query_stats(&region_info.0);
            self.store_stat
                .engine_total_query_num
                .add_query_stats(&region_info.0);
        }
    }

    fn handle_destroy_peer(&mut self, region_id: u64) {
        match self.region_peers.remove(&region_id) {
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
                let res: crate::Result<TimeStamp> =
                    if let Some(causal_ts_provider) = &causal_ts_provider {
                        causal_ts_provider
                            .async_flush()
                            .await
                            .map_err(|e| box_err!(e))
                    } else {
                        pd_client.get_tso().await.map_err(Into::into)
                    };

                match res {
                    Ok(ts) => {
                        concurrency_manager.update_max_ts(ts);
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
                        warn!(
                            "failed to update max timestamp for region {}: {:?}",
                            region_id, e
                        );
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
                        let msg = CasualMessage::QueryRegionLeaderResp { region, leader };
                        if let Err(e) = router.send(region_id, PeerMsg::CasualMessage(msg)) {
                            error!("send region info message failed"; "region_id" => region_id, "err" => ?e);
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    error!("get region failed"; "err" => ?e);
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
        if let Some(cpu_stats_sender) = self.stats_monitor.get_cpu_stats_sender() {
            if cpu_stats_sender.send(records.clone()).is_err() {
                warn!("send region cpu info failed, are we shutting down?")
            }
        }
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

                merge_bucket_stats(
                    &current.meta.keys,
                    &mut current.stats,
                    &buckets.meta.keys,
                    &buckets.stats,
                );
            })
            .or_insert_with(|| ReportBucket::new(buckets));
    }

    fn update_health_status(&mut self, status: ServingStatus) {
        self.curr_health_status = status;
        if let Some(health_service) = &self.health_service {
            health_service.set_serving_status("", status);
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
    type Task = Task<EK, ER>;

    fn run(&mut self, task: Task<EK, ER>) {
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
                callback,
            } => self.handle_ask_split(
                region,
                split_key,
                peer,
                right_derive,
                callback,
                String::from("ask_split"),
            ),
            Task::AskBatchSplit {
                region,
                split_keys,
                peer,
                right_derive,
                callback,
            } => Self::handle_ask_batch_split(
                self.router.clone(),
                self.scheduler.clone(),
                self.pd_client.clone(),
                region,
                split_keys,
                peer,
                right_derive,
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
                        if let Ok(Some(region)) =
                            pd_client.get_region_by_id(split_info.region_id).await
                        {
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
                                    Callback::None,
                                    String::from("auto_split"),
                                    remote.clone(),
                                );
                                return;
                            }
                            // Try to split the region on half within the given key range
                            // if there is no `split_key` been given.
                            if split_info.start_key.is_some() && split_info.end_key.is_some() {
                                let start_key = split_info.start_key.unwrap();
                                let end_key = split_info.end_key.unwrap();
                                let region_id = region.get_id();
                                let msg = CasualMessage::HalfSplitRegion {
                                    region_epoch: region.get_region_epoch().clone(),
                                    start_key: Some(start_key.clone()),
                                    end_key: Some(end_key.clone()),
                                    policy: pdpb::CheckPolicy::Scan,
                                    source: "auto_split",
                                    cb: Callback::None,
                                };
                                if let Err(e) = router.send(region_id, PeerMsg::CasualMessage(msg))
                                {
                                    error!("send auto half split request failed";
                                        "region_id" => region_id,
                                        "start_key" => log_wrappers::Value::key(&start_key),
                                        "end_key" => log_wrappers::Value::key(&end_key),
                                        "err" => ?e,
                                    );
                                }
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
                    cpu_usage,
                ) = {
                    let region_id = hb_task.region.get_id();
                    let peer_stat = self
                        .region_peers
                        .entry(region_id)
                        .or_insert_with(PeerStat::default);
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
                    let mut last_report_ts = peer_stat.last_region_report_ts;
                    peer_stat.last_region_report_written_bytes = hb_task.written_bytes;
                    peer_stat.last_region_report_written_keys = hb_task.written_keys;
                    peer_stat.last_region_report_read_bytes = peer_stat.read_bytes;
                    peer_stat.last_region_report_read_keys = peer_stat.read_keys;
                    peer_stat.last_region_report_query_stats = peer_stat.query_stats.clone();
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
                store_info,
                report,
                dr_autosync_status,
            } => self.handle_store_heartbeat(stats, store_info, report, dr_autosync_status),
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
            Task::UpdateSlowScore { id, duration } => self.slow_score.record(id, duration.sum()),
            Task::UpdateRegionCpuCollector(is_register) => {
                self.handle_update_region_cpu_collector(is_register)
            }
            Task::RegionCpuRecords(records) => self.handle_region_cpu_records(records),
            Task::ReportMinResolvedTs {
                store_id,
                min_resolved_ts,
            } => self.handle_report_min_resolved_ts(store_id, min_resolved_ts),
            Task::ReportBuckets(buckets) => {
                self.handle_report_region_buckets(buckets);
            }
        };
    }

    fn shutdown(&mut self) {
        self.stats_monitor.stop();
    }
}

impl<EK, ER, T> RunnableWithTimer for Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    fn on_timeout(&mut self) {
        // The health status is recovered to serving as long as any tick
        // does not timeout.
        if self.curr_health_status == ServingStatus::ServiceUnknown
            && self.slow_score.last_tick_finished
        {
            self.update_health_status(ServingStatus::Serving);
        }
        if !self.slow_score.last_tick_finished {
            self.slow_score.record_timeout();
        }
        let scheduler = self.scheduler.clone();
        let id = self.slow_score.last_tick_id + 1;
        self.slow_score.last_tick_id += 1;
        self.slow_score.last_tick_finished = false;

        if self.slow_score.last_tick_id % self.slow_score.round_ticks == 0 {
            // `last_update_time` is refreshed every round. If no update happens in a whole
            // round, we set the status to unknown.
            if self.curr_health_status == ServingStatus::Serving
                && self.slow_score.last_record_time < self.slow_score.last_update_time
            {
                self.update_health_status(ServingStatus::ServiceUnknown);
            }
            let slow_score = self.slow_score.update();
            STORE_SLOW_SCORE_GAUGE.set(slow_score);
        }

        let inspector = LatencyInspector::new(
            id,
            Box::new(move |id, duration| {
                let dur = duration.sum();

                STORE_INSPECT_DURTION_HISTOGRAM
                    .with_label_values(&["store_process"])
                    .observe(tikv_util::time::duration_to_sec(
                        duration.store_process_duration.unwrap(),
                    ));
                STORE_INSPECT_DURTION_HISTOGRAM
                    .with_label_values(&["store_wait"])
                    .observe(tikv_util::time::duration_to_sec(
                        duration.store_wait_duration.unwrap(),
                    ));
                STORE_INSPECT_DURTION_HISTOGRAM
                    .with_label_values(&["all"])
                    .observe(tikv_util::time::duration_to_sec(dur));
                if let Err(e) = scheduler.schedule(Task::UpdateSlowScore { id, duration }) {
                    warn!("schedule pd task failed"; "err" => ?e);
                }
            }),
        );
        let msg = StoreMsg::LatencyInspect {
            send_time: TiInstant::now(),
            inspector,
        };
        if let Err(e) = self.router.send_control(msg) {
            warn!("pd worker send latency inspecter failed"; "err" => ?e);
        }
    }

    fn get_interval(&self) -> Duration {
        self.slow_score.inspect_interval
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
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_region_id(new_region_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req.mut_split().set_right_derive(right_derive);
    req
}

fn new_batch_split_region_request(
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
        error!(
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

fn collect_engine_size<EK: KvEngine, ER: RaftEngine>(
    coprocessor_host: &CoprocessorHost<EK>,
    store_info: Option<&StoreInfo<EK, ER>>,
    snap_mgr_size: u64,
) -> Option<(u64, u64, u64)> {
    if let Some(engine_size) = coprocessor_host.on_compute_engine_size() {
        return Some((engine_size.capacity, engine_size.used, engine_size.avail));
    }
    let store_info = store_info.unwrap();
    let disk_stats = match fs2::statvfs(store_info.kv_engine.path()) {
        Err(e) => {
            error!(
                "get disk stat for rocksdb failed";
                "engine_path" => store_info.kv_engine.path(),
                "err" => ?e
            );
            return None;
        }
        Ok(stats) => stats,
    };
    let disk_cap = disk_stats.total_space();
    let capacity = if store_info.capacity == 0 || disk_cap < store_info.capacity {
        disk_cap
    } else {
        store_info.capacity
    };
    let used_size = snap_mgr_size
        + store_info
            .kv_engine
            .get_engine_used_size()
            .expect("kv engine used size")
        + store_info
            .raft_engine
            .get_engine_size()
            .expect("raft engine used size");
    let mut available = capacity.checked_sub(used_size).unwrap_or_default();
    // We only care about rocksdb SST file size, so we should check disk available
    // here.
    available = cmp::min(available, disk_stats.available_space());
    Some((capacity, used_size, available))
}

fn get_read_query_num(stat: &pdpb::QueryStats) -> u64 {
    stat.get_get() + stat.get_coprocessor() + stat.get_scan()
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use kvproto::{kvrpcpb, pdpb::QueryKind};
    use pd_client::{new_bucket_stats, BucketMeta};

    use super::*;

    const DEFAULT_TEST_STORE_ID: u64 = 1;

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn test_collect_stats() {
        use std::{sync::Mutex, time::Instant};

        use engine_test::{kv::KvTestEngine, raft::RaftTestEngine};
        use tikv_util::worker::LazyWorker;

        use crate::store::fsm::StoreMeta;

        struct RunnerTest {
            store_stat: Arc<Mutex<StoreStat>>,
            stats_monitor: StatsMonitor<KvTestEngine, RaftTestEngine>,
        }

        impl RunnerTest {
            fn new(
                interval: u64,
                scheduler: Scheduler<Task<KvTestEngine, RaftTestEngine>>,
                store_stat: Arc<Mutex<StoreStat>>,
            ) -> RunnerTest {
                let mut stats_monitor = StatsMonitor::new(
                    Duration::from_secs(interval),
                    Duration::from_secs(0),
                    scheduler,
                );
                let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
                let region_read_progress = store_meta.lock().unwrap().region_read_progress.clone();
                if let Err(e) =
                    stats_monitor.start(AutoSplitController::default(), region_read_progress, 1)
                {
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
            type Task = Task<KvTestEngine, RaftTestEngine>;

            fn run(&mut self, task: Task<KvTestEngine, RaftTestEngine>) {
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

    #[test]
    fn test_slow_score() {
        let mut slow_score = SlowScore::new(Duration::from_millis(500));
        slow_score.timeout_requests = 5;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(1.5),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 10;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(3.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 20;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(6.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 100;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(12.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 11;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(24.0),
            slow_score.update_impl(Duration::from_secs(10))
        );

        slow_score.timeout_requests = 0;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(19.0),
            slow_score.update_impl(Duration::from_secs(15))
        );

        slow_score.timeout_requests = 0;
        slow_score.total_requests = 100;
        assert_eq!(
            OrderedFloat(1.0),
            slow_score.update_impl(Duration::from_secs(57))
        );
    }

    use engine_test::{kv::KvTestEngine, raft::RaftTestEngine};
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
        let store_size = collect_engine_size::<KvTestEngine, RaftTestEngine>(&host, None, 0);
        let (cap, used, avail) = if let Some((cap, used, avail)) = store_size {
            (cap, used, avail)
        } else {
            panic!("store_size should not be none");
        };
        assert_eq!(cap, 444);
        assert_eq!(used, 111);
        assert_eq!(avail, 333);
    }
}
