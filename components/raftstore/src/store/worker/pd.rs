// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::{self, Sender};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use std::{cmp, io};

use futures::future::TryFutureExt;
use tokio::task::spawn_local;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::metapb;
use kvproto::pdpb;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, ChangePeerRequest, ChangePeerV2Request, RaftCmdRequest,
    SplitRequest,
};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::replication_modepb::RegionReplicationStatus;
use prometheus::local::LocalHistogram;
use raft::eraftpb::ConfChangeType;

use crate::store::cmd_resp::new_error;
use crate::store::metrics::*;
use crate::store::util::{is_epoch_stale, ConfChangeKind, KeysInfoFormatter};
use crate::store::worker::split_controller::{SplitInfo, TOP_N};
use crate::store::worker::{AutoSplitController, ReadStats};
use crate::store::Callback;
use crate::store::StoreInfo;
use crate::store::{CasualMessage, PeerMsg, RaftCommand, RaftRouter, StoreMsg};

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use futures::FutureExt;
use pd_client::metrics::*;
use pd_client::{Error, PdClient, RegionStat};
use tikv_util::metrics::ThreadInfoStatistics;
use tikv_util::time::UnixSecs;
use tikv_util::worker::{FutureRunnable as Runnable, FutureScheduler as Scheduler, Stopped};
use tokio::time::delay_for;

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
}

impl<E> FlowStatsReporter for Scheduler<Task<E>>
where
    E: KvEngine,
{
    fn report_read_stats(&self, read_stats: ReadStats) {
        if let Err(e) = self.schedule(Task::ReadStats { read_stats }) {
            error!("Failed to send read flow statistics"; "err" => ?e);
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
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub replication_status: Option<RegionReplicationStatus>,
}

/// Uses an asynchronous thread to tell PD something.
pub enum Task<E>
where
    E: KvEngine,
{
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        callback: Callback<E::Snapshot>,
    },
    AskBatchSplit {
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        callback: Callback<E::Snapshot>,
    },
    AutoSplit {
        split_infos: Vec<SplitInfo>,
    },
    Heartbeat(HeartbeatTask),
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        store_info: StoreInfo<E>,
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
        max_ts_sync_status: Arc<AtomicU64>,
    },
    QueryRegionLeader {
        region_id: u64,
    },
}

pub struct StoreStat {
    pub engine_total_bytes_read: u64,
    pub engine_total_keys_read: u64,
    pub engine_last_total_bytes_read: u64,
    pub engine_last_total_keys_read: u64,
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
    pub last_read_bytes: u64,
    pub last_read_keys: u64,
    pub last_written_bytes: u64,
    pub last_written_keys: u64,
    pub last_report_ts: UnixSecs,
    pub approximate_keys: u64,
    pub approximate_size: u64,
}

impl<E> Display for Task<E>
where
    E: KvEngine,
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
                log_wrappers::Value::key(&split_key),
            ),
            Task::AutoSplit {
                ref split_infos,
            } => write!(
                f,
                "auto split split regions, num is {}",
                split_infos.len(),
            ),
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
            } => write!(
                f,
                "validate peer {:?} with region {:?}",
                peer, region
            ),
            Task::ReadStats { ref read_stats } => {
                write!(f, "get the read statistics {:?}", read_stats)
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
                "get store's informations: cpu_usages {:?}, read_io_rates {:?}, write_io_rates {:?}",
                cpu_usages, read_io_rates, write_io_rates,
            ),
            Task::UpdateMaxTimestamp { region_id, .. } => write!(
                f,
                "update the max timestamp for region {} in the concurrency manager",
                region_id
            ),
            Task::QueryRegionLeader { region_id } => write!(
                f,
                "query the leader of region {}",
                region_id
            ),
        }
    }
}

const DEFAULT_QPS_INFO_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_COLLECT_INTERVAL: Duration = Duration::from_secs(1);

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

struct StatsMonitor<E>
where
    E: KvEngine,
{
    scheduler: Scheduler<Task<E>>,
    handle: Option<JoinHandle<()>>,
    timer: Option<Sender<bool>>,
    sender: Option<Sender<ReadStats>>,
    thread_info_interval: Duration,
    qps_info_interval: Duration,
    collect_interval: Duration,
}

impl<E> StatsMonitor<E>
where
    E: KvEngine,
{
    pub fn new(interval: Duration, scheduler: Scheduler<Task<E>>) -> Self {
        StatsMonitor {
            scheduler,
            handle: None,
            timer: None,
            sender: None,
            thread_info_interval: interval,
            qps_info_interval: cmp::min(DEFAULT_QPS_INFO_INTERVAL, interval),
            collect_interval: cmp::min(DEFAULT_COLLECT_INTERVAL, interval),
        }
    }

    // Collecting thread information and obtaining qps information for auto split.
    // They run together in the same thread by taking modulo at different intervals.
    pub fn start(
        &mut self,
        mut auto_split_controller: AutoSplitController,
    ) -> Result<(), io::Error> {
        if self.collect_interval < DEFAULT_COLLECT_INTERVAL {
            info!("it seems we are running tests, skip stats monitoring.");
            return Ok(());
        }
        let mut timer_cnt = 0; // to run functions with different intervals in a loop
        let collect_interval = self.collect_interval;
        if self.thread_info_interval < self.collect_interval {
            info!("running in test mode, skip starting monitor.");
            return Ok(());
        }
        let thread_info_interval = self
            .thread_info_interval
            .div_duration_f64(self.collect_interval) as i32;
        let qps_info_interval = self
            .qps_info_interval
            .div_duration_f64(self.collect_interval) as i32;
        let (tx, rx) = mpsc::channel();
        self.timer = Some(tx);

        let (sender, receiver) = mpsc::channel();
        self.sender = Some(sender);

        let scheduler = self.scheduler.clone();

        let h = Builder::new()
            .name(thd_name!("stats-monitor"))
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                let mut thread_stats = ThreadInfoStatistics::new();
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(collect_interval) {
                    if timer_cnt % thread_info_interval == 0 {
                        thread_stats.record();
                        let cpu_usages = convert_record_pairs(thread_stats.get_cpu_usages());
                        let read_io_rates = convert_record_pairs(thread_stats.get_read_io_rates());
                        let write_io_rates =
                            convert_record_pairs(thread_stats.get_write_io_rates());

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
                    if timer_cnt % qps_info_interval == 0 {
                        let mut others = vec![];
                        while let Ok(other) = receiver.try_recv() {
                            others.push(other);
                        }
                        let (top, split_infos) = auto_split_controller.flush(others);
                        auto_split_controller.clear();
                        let task = Task::AutoSplit { split_infos };
                        if let Err(e) = scheduler.schedule(task) {
                            error!(
                                "failed to send split infos to pd worker";
                                "err" => ?e,
                            );
                        }

                        for i in 0..TOP_N {
                            if i < top.len() {
                                READ_QPS_TOPN
                                    .with_label_values(&[&i.to_string()])
                                    .set(top[i] as f64);
                            } else {
                                READ_QPS_TOPN.with_label_values(&[&i.to_string()]).set(0.0);
                            }
                        }
                    }
                    // modules timer_cnt with the least common multiple of intervals to avoid overflow
                    timer_cnt = (timer_cnt + 1) % (qps_info_interval * thread_info_interval);
                    auto_split_controller.refresh_cfg();
                }
                tikv_alloc::remove_thread_memory_accessor();
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            drop(self.timer.take());
            drop(self.sender.take());
            if let Err(e) = h.join() {
                error!("join stats collector failed"; "err" => ?e);
            }
        }
    }

    pub fn get_sender(&self) -> &Option<Sender<ReadStats>> {
        &self.sender
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
    store_stat: StoreStat,
    is_hb_receiver_scheduled: bool,
    // Records the boot time.
    start_ts: UnixSecs,

    // use for Runner inner handle function to send Task to itself
    // actually it is the sender connected to Runner's Worker which
    // calls Runner's run() on Task received.
    scheduler: Scheduler<Task<EK>>,
    stats_monitor: StatsMonitor<EK>,

    concurrency_manager: ConcurrencyManager,
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    const INTERVAL_DIVISOR: u32 = 2;

    pub fn new(
        store_id: u64,
        pd_client: Arc<T>,
        router: RaftRouter<EK, ER>,
        scheduler: Scheduler<Task<EK>>,
        store_heartbeat_interval: Duration,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
    ) -> Runner<EK, ER, T> {
        let interval = store_heartbeat_interval / Self::INTERVAL_DIVISOR;
        let mut stats_monitor = StatsMonitor::new(interval, scheduler.clone());
        if let Err(e) = stats_monitor.start(auto_split_controller) {
            error!("failed to start stats collector, error = {:?}", e);
        }

        Runner {
            store_id,
            pd_client,
            router,
            is_hb_receiver_scheduled: false,
            region_peers: HashMap::default(),
            store_stat: StoreStat::default(),
            start_ts: UnixSecs::now(),
            scheduler,
            stats_monitor,
            concurrency_manager,
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
                    send_admin_request(&router, region_id, epoch, peer, req, callback)
                }
                Err(e) => {
                    warn!("failed to ask split";
                    "region_id" => region.get_id(),
                    "err" => ?e,
                    "task"=>task);
                }
            }
        };
        spawn_local(f);
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
        callback: Callback<EK::Snapshot>,
        task: String,
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
                    send_admin_request(&router, region_id, epoch, peer, req, callback)
                }
                // When rolling update, there might be some old version tikvs that don't support batch split in cluster.
                // In this situation, PD version check would refuse `ask_batch_split`.
                // But if update time is long, it may cause large Regions, so call `ask_split` instead.
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
                    if let Err(Stopped(t)) = scheduler.schedule(task) {
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
        spawn_local(f);
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

        let f = self
            .pd_client
            .region_heartbeat(term, region.clone(), peer, region_stat, replication_status)
            .map_err(move |e| {
                debug!(
                    "failed to send heartbeat";
                    "region_id" => region.get_id(),
                    "err" => ?e
                );
            });
        spawn_local(f);
    }

    fn handle_store_heartbeat(&mut self, mut stats: pdpb::StoreStats, store_info: StoreInfo<EK>) {
        let disk_stats = match fs2::statvfs(store_info.engine.path()) {
            Err(e) => {
                error!(
                    "get disk stat for rocksdb failed";
                    "engine_path" => store_info.engine.path(),
                    "err" => ?e
                );
                return;
            }
            Ok(stats) => stats,
        };

        let disk_cap = disk_stats.total_space();
        let capacity = if store_info.capacity == 0 || disk_cap < store_info.capacity {
            disk_cap
        } else {
            store_info.capacity
        };
        stats.set_capacity(capacity);

        // already include size of snapshot files
        let used_size =
            stats.get_used_size() + store_info.engine.get_engine_used_size().expect("cf");
        stats.set_used_size(used_size);

        let mut available = if capacity > used_size {
            capacity - used_size
        } else {
            warn!("no available space");
            0
        };

        // We only care about rocksdb SST file size, so we should check disk available here.
        if available > disk_stats.free_space() {
            available = disk_stats.free_space();
        }

        stats.set_available(available);
        stats.set_bytes_read(
            self.store_stat.engine_total_bytes_read - self.store_stat.engine_last_total_bytes_read,
        );
        stats.set_keys_read(
            self.store_stat.engine_total_keys_read - self.store_stat.engine_last_total_keys_read,
        );

        stats.set_cpu_usages(self.store_stat.store_cpu_usages.clone().into());
        stats.set_read_io_rates(self.store_stat.store_read_io_rates.clone().into());
        stats.set_write_io_rates(self.store_stat.store_write_io_rates.clone().into());

        let mut interval = pdpb::TimeInterval::default();
        interval.set_start_timestamp(self.store_stat.last_report_ts.into_inner());
        stats.set_interval(interval);
        self.store_stat.engine_last_total_bytes_read = self.store_stat.engine_total_bytes_read;
        self.store_stat.engine_last_total_keys_read = self.store_stat.engine_total_keys_read;
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

        let router = self.router.clone();
        let resp = self.pd_client.store_heartbeat(stats);
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    if let Some(status) = resp.replication_status.take() {
                        let _ = router.send_control(StoreMsg::UpdateReplicationMode(status));
                    }
                }
                Err(e) => {
                    error!("store heartbeat failed"; "err" => ?e);
                }
            }
        };
        spawn_local(f);
    }

    fn handle_report_batch_split(&self, regions: Vec<metapb::Region>) {
        let f = self.pd_client.report_batch_split(regions).map_err(|e| {
            warn!("report split failed"; "err" => ?e);
        });
        spawn_local(f);
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
        spawn_local(f);
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
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None);
                } else if resp.has_change_peer_v2() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["change peer"])
                        .inc();

                    let mut change_peer_v2 = resp.take_change_peer_v2();
                    info!(
                        "try to change peer";
                        "region_id" => region_id,
                        "changes" => ?change_peer_v2.get_changes(),
                        "kind" => ?ConfChangeKind::confchange_kind(change_peer_v2.get_changes().len()),
                    );
                    let req = new_change_peer_v2_request(change_peer_v2.take_changes().into());
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None);
                } else if resp.has_transfer_leader() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["transfer leader"])
                        .inc();

                    let mut transfer_leader = resp.take_transfer_leader();
                    info!(
                        "try to transfer leader";
                        "region_id" => region_id,
                        "from_peer" => ?peer,
                        "to_peer" => ?transfer_leader.get_peer()
                    );
                    let req = new_transfer_leader_request(transfer_leader.take_peer());
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None);
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
                            policy: split_region.get_policy(),
                            source: "pd",
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
                    send_admin_request(&router, region_id, epoch, peer, req, Callback::None)
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
        spawn_local(f);
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

            region_info.approximate_key = peer_stat.approximate_keys;
            region_info.approximate_size = peer_stat.approximate_size;
        }
        if !read_stats.region_infos.is_empty() {
            if let Some(sender) = self.stats_monitor.get_sender() {
                if sender.send(read_stats).is_err() {
                    warn!("send read_stats failed, are we shutting down?")
                }
            }
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
        max_ts_sync_status: Arc<AtomicU64>,
    ) {
        let pd_client = self.pd_client.clone();
        let concurrency_manager = self.concurrency_manager.clone();
        let f = async move {
            let mut success = false;
            while max_ts_sync_status.load(Ordering::SeqCst) == initial_status {
                match pd_client.get_tso().await {
                    Ok(ts) => {
                        concurrency_manager.update_max_ts(ts);
                        // Set the least significant bit to 1 to mark it as synced.
                        let old_value = max_ts_sync_status.compare_and_swap(
                            initial_status,
                            initial_status | 1,
                            Ordering::SeqCst,
                        );
                        success = old_value == initial_status;
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

        let delay = (|| {
            fail_point!("delay_update_max_ts", |_| true);
            false
        })();

        if delay {
            info!("[failpoint] delay update max ts for 1s"; "region_id" => region_id);
            spawn_local(delay_for(Duration::from_secs(1)).then(|_| f));
        } else {
            spawn_local(f);
        }
    }

    fn handle_query_region_leader(&self, region_id: u64) {
        let router = self.router.clone();
        let resp = self.pd_client.get_region_leader_by_id(region_id);
        let f = async move {
            match resp.await {
                Ok(Some((region, leader))) => {
                    let msg = CasualMessage::QueryRegionLeaderResp { region, leader };
                    if let Err(e) = router.send(region_id, PeerMsg::CasualMessage(msg)) {
                        error!("send region info message failed"; "region_id" => region_id, "err" => ?e);
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    error!("get region failed"; "err" => ?e);
                }
            }
        };
        spawn_local(f);
    }
}

impl<EK, ER, T> Runnable<Task<EK>> for Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient,
{
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
            ),
            Task::AutoSplit { split_infos } => {
                let pd_client = self.pd_client.clone();
                let router = self.router.clone();
                let scheduler = self.scheduler.clone();

                let f = async move {
                    for split_info in split_infos {
                        if let Ok(Some(region)) =
                            pd_client.get_region_by_id(split_info.region_id).await
                        {
                            Self::handle_ask_batch_split(
                                router.clone(),
                                scheduler.clone(),
                                pd_client.clone(),
                                region,
                                vec![split_info.split_key],
                                split_info.peer,
                                true,
                                Callback::None,
                                String::from("auto_split"),
                            );
                        }
                    }
                };
                spawn_local(f);
            }

            Task::Heartbeat(hb_task) => {
                let (
                    read_bytes_delta,
                    read_keys_delta,
                    written_bytes_delta,
                    written_keys_delta,
                    last_report_ts,
                ) = {
                    let peer_stat = self
                        .region_peers
                        .entry(hb_task.region.get_id())
                        .or_insert_with(PeerStat::default);
                    peer_stat.approximate_size = hb_task.approximate_size;
                    peer_stat.approximate_keys = hb_task.approximate_keys;
                    let read_bytes_delta = peer_stat.read_bytes - peer_stat.last_read_bytes;
                    let read_keys_delta = peer_stat.read_keys - peer_stat.last_read_keys;
                    let written_bytes_delta = hb_task.written_bytes - peer_stat.last_written_bytes;
                    let written_keys_delta = hb_task.written_keys - peer_stat.last_written_keys;
                    let mut last_report_ts = peer_stat.last_report_ts;
                    peer_stat.last_written_bytes = hb_task.written_bytes;
                    peer_stat.last_written_keys = hb_task.written_keys;
                    peer_stat.last_read_bytes = peer_stat.read_bytes;
                    peer_stat.last_read_keys = peer_stat.read_keys;
                    peer_stat.last_report_ts = UnixSecs::now();
                    if last_report_ts.is_zero() {
                        last_report_ts = self.start_ts;
                    }
                    (
                        read_bytes_delta,
                        read_keys_delta,
                        written_bytes_delta,
                        written_keys_delta,
                        last_report_ts,
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
                        approximate_size: hb_task.approximate_size,
                        approximate_keys: hb_task.approximate_keys,
                        last_report_ts,
                    },
                    hb_task.replication_status,
                )
            }
            Task::StoreHeartbeat { stats, store_info } => {
                self.handle_store_heartbeat(stats, store_info)
            }
            Task::ReportBatchSplit { regions } => self.handle_report_batch_split(regions),
            Task::ValidatePeer { region, peer } => self.handle_validate_peer(region, peer),
            Task::ReadStats { read_stats } => self.handle_read_stats(read_stats),
            Task::DestroyPeer { region_id } => self.handle_destroy_peer(region_id),
            Task::StoreInfos {
                cpu_usages,
                read_io_rates,
                write_io_rates,
            } => self.handle_store_infos(cpu_usages, read_io_rates, write_io_rates),
            Task::UpdateMaxTimestamp {
                region_id,
                initial_status,
                max_ts_sync_status,
            } => self.handle_update_max_timestamp(region_id, initial_status, max_ts_sync_status),
            Task::QueryRegionLeader { region_id } => self.handle_query_region_leader(region_id),
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

fn new_change_peer_v2_request(changes: Vec<pdpb::ChangePeer>) -> AdminRequest {
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

fn new_transfer_leader_request(peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
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

    if let Err(e) = router.send_raft_command(RaftCommand::new(req, callback)) {
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

#[cfg(not(target_os = "macos"))]
#[cfg(test)]
mod tests {
    use engine_test::kv::KvTestEngine;
    use std::sync::Mutex;
    use std::time::Instant;
    use tikv_util::worker::FutureWorker;

    use super::*;

    struct RunnerTest {
        store_stat: Arc<Mutex<StoreStat>>,
        stats_monitor: StatsMonitor<KvTestEngine>,
    }

    impl RunnerTest {
        fn new(
            interval: u64,
            scheduler: Scheduler<Task<KvTestEngine>>,
            store_stat: Arc<Mutex<StoreStat>>,
        ) -> RunnerTest {
            let mut stats_monitor = StatsMonitor::new(Duration::from_secs(interval), scheduler);

            if let Err(e) = stats_monitor.start(AutoSplitController::default()) {
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

    impl Runnable<Task<KvTestEngine>> for RunnerTest {
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

    #[test]
    fn test_collect_stats() {
        let mut pd_worker = FutureWorker::new("test-pd-worker");
        let store_stat = Arc::new(Mutex::new(StoreStat::default()));
        let runner = RunnerTest::new(1, pd_worker.scheduler(), Arc::clone(&store_stat));
        pd_worker.start(runner).unwrap();

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
}
