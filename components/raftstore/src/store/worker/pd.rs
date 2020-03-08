// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use futures::sync::oneshot;
use futures::Future;
use tokio_core::reactor::Handle;
use tokio_timer::Delay;

use engine::rocks::util::*;
use engine::rocks::DB;
use engine_rocks::RocksEngine;
use fs2;
use kvproto::metapb;
use kvproto::pdpb;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest, SplitRequest};
use kvproto::raft_serverpb::RaftMessage;
use prometheus::local::LocalHistogram;
use raft::eraftpb::ConfChangeType;

use crate::coprocessor::{get_region_approximate_keys, get_region_approximate_size};
use crate::store::cmd_resp::new_error;
use crate::store::metrics::*;
use crate::store::util::is_epoch_stale;
use crate::store::util::KeysInfoFormatter;
use crate::store::Callback;
use crate::store::StoreInfo;
use crate::store::{CasualMessage, PeerMsg, RaftCommand, RaftRouter, SignificantMsg};
use pd_client::metrics::*;
use pd_client::{ConfigClient, Error, PdClient, RegionStat};
use rand::Rng;
use tikv_util::collections::HashMap;
use tikv_util::metrics::ThreadInfoStatistics;
use tikv_util::time::UnixSecs;
use tikv_util::worker::{FutureRunnable as Runnable, FutureScheduler as Scheduler, Stopped};

use txn_types::Key;
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
    // Reports read flow statistics, the argument `read_stats` is a hash map
    // saves the flow statistics of different region.
    // TODO: maybe we need to return a Result later?
    fn report_read_stats(&self, read_stats: HashMap<u64, FlowStatistics>);

    fn report_qps_stats(&self, hub: SplitHub);
}

impl FlowStatsReporter for Scheduler<Task> {
    fn report_read_stats(&self, read_stats: HashMap<u64, FlowStatistics>) {
        if let Err(e) = self.schedule(Task::ReadStats { read_stats }) {
            error!("Failed to send read flow statistics"; "err" => ?e);
        }
    }
    fn report_qps_stats(&self, hub: SplitHub) {
        if let Err(e) = self.schedule(Task::QpsStats { hub }) {
            error!("Failed to send qps statistics"; "err" => ?e);
        }
    }
}

pub trait DynamicConfig: Send + 'static {
    fn refresh(&mut self, cfg_client: &dyn ConfigClient);
    fn refresh_interval(&self) -> Duration;
    fn get(&self) -> String;
}

pub struct SplitInfo {
    pub region_id: u64,
    pub split_key: Vec<u8>,
    pub peer: metapb::Peer,
}

/// Uses an asynchronous thread to tell PD something.
pub enum Task {
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        callback: Callback<RocksEngine>,
    },
    AskBatchSplit {
        region: metapb::Region,
        split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        // If true, right Region derives origin region_id.
        right_derive: bool,
        callback: Callback<RocksEngine>,
    },
    ToAskSplit {
        split_infos: Vec<SplitInfo>,
    },
    Heartbeat {
        term: u64,
        region: metapb::Region,
        peer: metapb::Peer,
        down_peers: Vec<pdpb::PeerStats>,
        pending_peers: Vec<metapb::Peer>,
        written_bytes: u64,
        written_keys: u64,
        approximate_size: Option<u64>,
        approximate_keys: Option<u64>,
    },
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        store_info: StoreInfo,
    },
    ReportBatchSplit {
        regions: Vec<metapb::Region>,
    },
    ValidatePeer {
        region: metapb::Region,
        peer: metapb::Peer,
        merge_source: Option<u64>,
    },
    ReadStats {
        read_stats: HashMap<u64, FlowStatistics>,
    },
    QpsStats {
        hub: SplitHub,
    },
    DestroyPeer {
        region_id: u64,
    },
    StoreInfos {
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    },
    RefreshConfig,
    GetConfig {
        cfg_sender: oneshot::Sender<String>,
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
}

impl Display for Task {
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
                hex::encode_upper(&split_key),
            ),
            Task::ToAskSplit {
                ..
            } => write!(
                f,
                "to ask split region"
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
            Task::Heartbeat {
                ref region,
                ref peer,
                ..
            } => write!(
                f,
                "heartbeat for region {:?}, leader {}",
                region,
                peer.get_id()
            ),
            Task::StoreHeartbeat { ref stats, .. } => {
                write!(f, "store heartbeat stats: {:?}", stats)
            }
            Task::ReportBatchSplit { ref regions } => write!(f, "report split {:?}", regions),
            Task::ValidatePeer {
                ref region,
                ref peer,
                ref merge_source,
            } => write!(
                f,
                "validate peer {:?} with region {:?}, merge_source {:?}",
                peer, region, merge_source
            ),
            Task::ReadStats { ref read_stats } => {
                write!(f, "get the read statistics {:?}", read_stats)
            }
            Task::QpsStats { ref hub } => {
                write!(f, "get the qps statistics {:?}", hub.region_qps)
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
            Task::RefreshConfig => write!(f, "refresh config"),
            Task::GetConfig { .. } => write!(f, "get config"),
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

const DEFAULT_QPS_INFO_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_COLLECT_INTERVAL: Duration = Duration::from_secs(1);

struct StatsMonitor {
    scheduler: Scheduler<Task>,
    handle: Option<JoinHandle<()>>,
    timer: Option<Sender<bool>>,
    sender: Option<Sender<SplitHub>>,
    thread_info_interval: Duration,
    qps_info_interval: Duration,
    collect_interval: Duration,
}

impl StatsMonitor {
    pub fn new(interval: Duration, scheduler: Scheduler<Task>) -> Self {
        StatsMonitor {
            scheduler,
            handle: None,
            timer: None,
            sender: None,
            thread_info_interval: interval,
            qps_info_interval: DEFAULT_QPS_INFO_INTERVAL,
            collect_interval: DEFAULT_COLLECT_INTERVAL,
        }
    }

    pub fn start(&mut self, hub_config: SplitHubConfig) -> Result<(), io::Error> {
        let mut timer_cnt = 0;
        let collect_interval = self.collect_interval;
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
        let mut unify_hub = SplitHub::new();
        unify_hub.qps_threshold = hub_config.qps_threshold;
        unify_hub.split_score = hub_config.split_score;

        let h = Builder::new()
            .name(thd_name!("stats-monitor"))
            .spawn(move || {
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
                        while let Ok(mut other_hub) = receiver.try_recv() {
                            unify_hub.update(&mut other_hub);
                        }

                        let (top, split_infos) = unify_hub.flush();
                        let task = Task::ToAskSplit { split_infos };
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
                    timer_cnt = (timer_cnt + 1) % (qps_info_interval * thread_info_interval);
                }
            })?;

        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        drop(self.timer.take().unwrap());
        drop(self.sender.take().unwrap());
        if let Err(e) = h.unwrap().join() {
            error!("join stats collector failed"; "err" => ?e);
            return;
        }
    }
}

pub struct Runner<T: PdClient + ConfigClient> {
    store_id: u64,
    pd_client: Arc<T>,
    config_handler: Box<dyn DynamicConfig>,
    router: RaftRouter<RocksEngine>,
    db: Arc<DB>,
    region_peers: HashMap<u64, PeerStat>,
    store_stat: StoreStat,
    is_hb_receiver_scheduled: bool,
    // Records the boot time.
    start_ts: UnixSecs,

    // use for Runner inner handle function to send Task to itself
    // actually it is the sender connected to Runner's Worker which
    // calls Runner's run() on Task received.
    scheduler: Scheduler<Task>,
    stats_monitor: StatsMonitor,
}

impl<T: PdClient + ConfigClient> Runner<T> {
    const INTERVAL_DIVISOR: u32 = 2;

    pub fn new(
        store_id: u64,
        pd_client: Arc<T>,
        config_handler: Box<dyn DynamicConfig>,
        router: RaftRouter<RocksEngine>,
        db: Arc<DB>,
        scheduler: Scheduler<Task>,
        store_heartbeat_interval: u64,
        hub_config: SplitHubConfig,
    ) -> Runner<T> {
        let interval = Duration::from_secs(store_heartbeat_interval) / Self::INTERVAL_DIVISOR;
        let mut stats_monitor = StatsMonitor::new(interval, scheduler.clone());
        if let Err(e) = stats_monitor.start(hub_config) {
            error!("failed to start stats collector, error = {:?}", e);
        }

        Runner {
            store_id,
            pd_client,
            config_handler,
            router,
            db,
            is_hb_receiver_scheduled: false,
            region_peers: HashMap::default(),
            store_stat: StoreStat::default(),
            start_ts: UnixSecs::now(),
            scheduler,
            stats_monitor,
        }
    }

    fn handle_ask_split(
        &self,
        handle: &Handle,
        mut region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
        right_derive: bool,
        callback: Callback<RocksEngine>,
    ) {
        let router = self.router.clone();
        let f = self.pd_client.ask_split(region.clone()).then(move |resp| {
            match resp {
                Ok(mut resp) => {
                    info!(
                        "try to split region";
                        "region_id" => region.get_id(),
                        "new_region_id" => resp.get_new_region_id(),
                        "region" => ?region
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
                    debug!("failed to ask split";
                    "region_id" => region.get_id(),
                    "err" => ?e);
                }
            }
            Ok(())
        });
        handle.spawn(f)
    }

    fn handle_ask_batch_split(
        &self,
        handle: &Handle,
        mut region: metapb::Region,
        mut split_keys: Vec<Vec<u8>>,
        peer: metapb::Peer,
        right_derive: bool,
        callback: Callback<RocksEngine>,
    ) {
        let router = self.router.clone();
        let scheduler = self.scheduler.clone();
        let f = self
            .pd_client
            .ask_batch_split(region.clone(), split_keys.len())
            .then(move |resp| {
                match resp {
                    Ok(mut resp) => {
                        info!(
                            "try to batch split region";
                            "region_id" => region.get_id(),
                            "new_region_ids" => ?resp.get_ids(),
                            "region" => ?region,
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
                        debug!(
                            "ask batch split failed";
                            "region_id" => region.get_id(),
                            "err" => ?e,
                        );
                    }
                }
                Ok(())
            });
        handle.spawn(f)
    }

    fn handle_heartbeat(
        &self,
        handle: &Handle,
        term: u64,
        region: metapb::Region,
        peer: metapb::Peer,
        region_stat: RegionStat,
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
            .region_heartbeat(term, region.clone(), peer, region_stat)
            .map_err(move |e| {
                debug!(
                    "failed to send heartbeat";
                    "region_id" => region.get_id(),
                    "err" => ?e
                );
            });
        handle.spawn(f);
    }

    fn handle_store_heartbeat(
        &mut self,
        handle: &Handle,
        mut stats: pdpb::StoreStats,
        store_info: StoreInfo,
    ) {
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
            stats.get_used_size() + get_engine_used_size(Arc::clone(&store_info.engine));
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

        let f = self.pd_client.store_heartbeat(stats).map_err(|e| {
            error!("store heartbeat failed"; "err" => ?e);
        });
        handle.spawn(f);
    }

    fn handle_report_batch_split(&self, handle: &Handle, regions: Vec<metapb::Region>) {
        let f = self.pd_client.report_batch_split(regions).map_err(|e| {
            debug!("report split failed"; "err" => ?e);
        });
        handle.spawn(f);
    }

    fn handle_validate_peer(
        &self,
        handle: &Handle,
        local_region: metapb::Region,
        peer: metapb::Peer,
        merge_source: Option<u64>,
    ) {
        let router = self.router.clone();
        let f = self
            .pd_client
            .get_region_by_id(local_region.get_id())
            .then(move |resp| {
                match resp {
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
                            return Ok(());
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
                            if let Some(source) = merge_source {
                                send_merge_fail(&router, source, peer);
                            } else {
                                send_destroy_peer_message(&router, local_region, peer, pd_region);
                            }
                            return Ok(());
                        }
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
                    Ok(None) => {
                        // splitted Region has not yet reported to PD.
                        // TODO: handle merge
                    }
                    Err(e) => {
                        error!("get region failed"; "err" => ?e);
                    }
                }
                Ok(())
            });
        handle.spawn(f);
    }

    fn schedule_heartbeat_receiver(&mut self, handle: &Handle) {
        let router = self.router.clone();
        let store_id = self.store_id;
        let f = self
            .pd_client
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
                        }
                    } else {
                        CasualMessage::HalfSplitRegion {
                            region_epoch: epoch,
                            policy: split_region.get_policy(),
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
            })
            .map_err(|e| panic!("unexpected error: {:?}", e))
            .map(move |_| {
                info!(
                    "region heartbeat response handler exit";
                    "store_id" => store_id,
                )
            });
        handle.spawn(f);
        self.is_hb_receiver_scheduled = true;
    }

    fn handle_read_stats(&mut self, read_stats: HashMap<u64, FlowStatistics>) {
        for (region_id, stats) in read_stats {
            let peer_stat = self
                .region_peers
                .entry(region_id)
                .or_insert_with(PeerStat::default);
            peer_stat.read_bytes += stats.read_bytes as u64;
            peer_stat.read_keys += stats.read_keys as u64;
            self.store_stat.engine_total_bytes_read += stats.read_bytes as u64;
            self.store_stat.engine_total_keys_read += stats.read_keys as u64;
        }
    }

    fn handle_qps_stats(&mut self, hub: SplitHub) {
        if let Some(sender) = &self.stats_monitor.sender {
            sender.send(hub).unwrap();
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

    fn handle_refresh_config(&mut self, handle: &Handle) {
        self.config_handler.refresh(self.pd_client.as_ref() as _);

        let scheduler = self.scheduler.clone();
        let when = Instant::now() + self.config_handler.refresh_interval();
        let f = Delay::new(when)
            .map_err(|e| warn!("timeout timer delay errored"; "err" => ?e))
            .then(move |_| {
                if let Err(e) = scheduler.schedule(Task::RefreshConfig) {
                    error!("failed to schedule refresh config task"; "err" => ?e)
                }
                Ok(())
            });
        handle.spawn(f);
    }

    fn handle_get_config(&self, cfg_sender: oneshot::Sender<String>) {
        let cfg = self.config_handler.get();
        let _ = cfg_sender
            .send(cfg)
            .map_err(|_| error!("failed to send config"));
    }
}

impl<T: PdClient + ConfigClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task, handle: &Handle) {
        debug!("executing task"; "task" => %task);

        if !self.is_hb_receiver_scheduled {
            self.schedule_heartbeat_receiver(handle);
        }

        match task {
            Task::AskSplit {
                region,
                split_key,
                peer,
                right_derive,
                callback,
            } => self.handle_ask_split(handle, region, split_key, peer, right_derive, callback),
            Task::AskBatchSplit {
                region,
                split_keys,
                peer,
                right_derive,
                callback,
            } => self.handle_ask_batch_split(
                handle,
                region,
                split_keys,
                peer,
                right_derive,
                callback,
            ),
            Task::ToAskSplit { split_infos } => {
                for split_info in split_infos {
                    if let Ok(Some(region)) =
                        self.pd_client.get_region_by_id(split_info.region_id).wait()
                    {
                        self.handle_ask_split(
                            handle,
                            region,
                            split_info.split_key,
                            split_info.peer,
                            true,
                            Callback::None,
                        );
                    }
                }
            }

            Task::Heartbeat {
                term,
                region,
                peer,
                down_peers,
                pending_peers,
                written_bytes,
                written_keys,
                approximate_size,
                approximate_keys,
            } => {
                let approximate_size = approximate_size.unwrap_or_else(|| {
                    get_region_approximate_size(&self.db, &region).unwrap_or_default()
                });
                let approximate_keys = approximate_keys.unwrap_or_else(|| {
                    get_region_approximate_keys(&self.db, &region).unwrap_or_default()
                });
                let (
                    read_bytes_delta,
                    read_keys_delta,
                    written_bytes_delta,
                    written_keys_delta,
                    last_report_ts,
                ) = {
                    let peer_stat = self
                        .region_peers
                        .entry(region.get_id())
                        .or_insert_with(PeerStat::default);
                    let read_bytes_delta = peer_stat.read_bytes - peer_stat.last_read_bytes;
                    let read_keys_delta = peer_stat.read_keys - peer_stat.last_read_keys;
                    let written_bytes_delta = written_bytes - peer_stat.last_written_bytes;
                    let written_keys_delta = written_keys - peer_stat.last_written_keys;
                    let mut last_report_ts = peer_stat.last_report_ts;
                    peer_stat.last_written_bytes = written_bytes;
                    peer_stat.last_written_keys = written_keys;
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
                    handle,
                    term,
                    region,
                    peer,
                    RegionStat {
                        down_peers,
                        pending_peers,
                        written_bytes: written_bytes_delta,
                        written_keys: written_keys_delta,
                        read_bytes: read_bytes_delta,
                        read_keys: read_keys_delta,
                        approximate_size,
                        approximate_keys,
                        last_report_ts,
                    },
                )
            }
            Task::StoreHeartbeat { stats, store_info } => {
                self.handle_store_heartbeat(handle, stats, store_info)
            }
            Task::ReportBatchSplit { regions } => self.handle_report_batch_split(handle, regions),
            Task::ValidatePeer {
                region,
                peer,
                merge_source,
            } => self.handle_validate_peer(handle, region, peer, merge_source),
            Task::ReadStats { read_stats } => self.handle_read_stats(read_stats),
            Task::QpsStats { hub } => self.handle_qps_stats(hub),
            Task::DestroyPeer { region_id } => self.handle_destroy_peer(region_id),
            Task::StoreInfos {
                cpu_usages,
                read_io_rates,
                write_io_rates,
            } => self.handle_store_infos(cpu_usages, read_io_rates, write_io_rates),
            Task::RefreshConfig => self.handle_refresh_config(handle),
            Task::GetConfig { cfg_sender } => self.handle_get_config(cfg_sender),
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

fn send_admin_request(
    router: &RaftRouter<RocksEngine>,
    region_id: u64,
    epoch: metapb::RegionEpoch,
    peer: metapb::Peer,
    request: AdminRequest,
    callback: Callback<RocksEngine>,
) {
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

/// Sends merge fail message to gc merge source.
fn send_merge_fail(router: &RaftRouter<RocksEngine>, source_region_id: u64, target: metapb::Peer) {
    let target_id = target.get_id();
    if let Err(e) = router.force_send(
        source_region_id,
        PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
            target,
            stale: true,
        }),
    ) {
        error!(
            "source region report merge failed";
            "region_id" => source_region_id, "targe_region_id" => target_id, "err" => ?e,
        );
    }
}

/// Sends a raft message to destroy the specified stale Peer
fn send_destroy_peer_message(
    router: &RaftRouter<RocksEngine>,
    local_region: metapb::Region,
    peer: metapb::Peer,
    pd_region: metapb::Region,
) {
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

const DEFAULT_QPS_THRESHOLD: u32 = 500;
const DETECT_TIMES: u32 = 10;
pub const TOP_N: usize = 10;
const DETECT_INTERVAL: Duration = Duration::from_secs(1);
const MIN_SAMPLE_NUM: i32 = 100;
const DEFAULT_SPLIT_SCORE: f64 = 0.25;

pub struct Sample {
    pub key: Vec<u8>,
    pub left: i32,
    pub contained: i32,
    pub right: i32,
}

impl Sample {
    fn new(key: &[u8]) -> Sample {
        Sample {
            key: key.to_owned(),
            left: 0,
            contained: 0,
            right: 0,
        }
    }
}

pub struct KeyRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub qps: u64,
}

impl KeyRange {
    fn new(start_key: &[u8], end_key: &[u8]) -> KeyRange {
        KeyRange {
            start_key: start_key.to_owned(),
            end_key: end_key.to_owned(),
            qps: 0,
        }
    }
}

pub struct Recorder {
    pub samples: Vec<Sample>,
    pub times: u32,
    pub count: u64,
    pub create_time: SystemTime,
}

impl Recorder {
    fn new() -> Recorder {
        Recorder {
            samples: vec![],
            times: 0,
            count: 0,
            create_time: SystemTime::now(),
        }
    }

    fn record(&mut self, key_ranges: &[KeyRange]) {
        self.times += 1;
        let mut rng = rand::thread_rng();
        for key_range in key_ranges.iter() {
            self.count += 1;
            if self.samples.len() < 20 {
                self.samples.push(Sample::new(&key_range.start_key));
            } else {
                self.sample(key_range);
                let i = rng.gen_range(0, self.count) as usize;
                if i < 20 {
                    self.samples[i] = Sample::new(&key_range.start_key);
                }
            }
        }
    }

    fn sample(&mut self, key_range: &KeyRange) {
        for mut sample in self.samples.iter_mut() {
            // we think key range is full open interval, such as (a,b) or (a,"").
            if sample.key.cmp(&key_range.start_key) != Ordering::Greater {
                sample.right += 1;
            } else if key_range.end_key.is_empty()
                || sample.key.cmp(&key_range.end_key) != Ordering::Less
            {
                sample.left += 1;
            } else {
                sample.contained += 1;
            }
        }
    }

    fn split_key(&self, split_score: f64, min_sample_num: i32) -> Vec<u8> {
        if self.times < DETECT_TIMES {
            return vec![];
        }
        let mut best_index: i32 = -1;
        let mut best_score = split_score;
        for index in 0..self.samples.len() {
            let sample = &self.samples[index];
            if sample.contained + sample.left + sample.right < min_sample_num {
                continue;
            }
            let diff = (sample.left - sample.right) as f64;
            let balance_score = diff.abs() / (sample.left + sample.right) as f64;
            if balance_score < best_score {
                best_index = index as i32;
                best_score = balance_score;
            }
        }
        if best_index >= 0 {
            return self.samples[best_index as usize].key.clone();
        }
        return vec![];
    }
}

#[derive(Debug)]
pub struct RegionInfo {
    pub peer: metapb::Peer,
    pub qps: u32,
}

impl RegionInfo {
    fn new() -> RegionInfo {
        RegionInfo {
            qps: 0,
            peer: metapb::Peer::default(),
        }
    }

    fn add(&mut self, peer: &metapb::Peer, num: u32) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
        self.qps += num;
    }
}

pub struct SplitHubConfig {
    pub qps_threshold: u32,
    pub split_score: f64,
}

impl SplitHubConfig {
    pub fn default() -> SplitHubConfig {
        SplitHubConfig {
            qps_threshold: DEFAULT_QPS_THRESHOLD,
            split_score: DEFAULT_SPLIT_SCORE,
        }
    }
}

pub struct SplitHub {
    pub region_qps: HashMap<u64, RegionInfo>,
    pub region_keys: HashMap<u64, Vec<KeyRange>>,
    pub region_recorder: HashMap<u64, Recorder>,
    pub qps_threshold: u32,
    pub split_score: f64,
    pub interval: Duration,
    pub min_sample_num: i32,
}

impl SplitHub {
    pub fn new() -> SplitHub {
        SplitHub {
            region_qps: HashMap::default(),
            region_keys: HashMap::default(),
            region_recorder: HashMap::default(),
            qps_threshold: DEFAULT_QPS_THRESHOLD,
            split_score: DEFAULT_SPLIT_SCORE,
            interval: DETECT_INTERVAL,
            min_sample_num: MIN_SAMPLE_NUM,
        }
    }

    fn add_qps(&mut self, region_id: u64, peer: &metapb::Peer, num: u32) {
        let region_info = self
            .region_qps
            .entry(region_id)
            .or_insert_with(RegionInfo::new);
        region_info.add(peer, num);
    }

    fn add_key_range(&mut self, region_id: u64, start_key: &[u8], end_key: &[u8]) {
        let key_ranges = self.region_keys.entry(region_id).or_insert_with(|| vec![]);
        (*key_ranges).push(KeyRange::new(start_key, end_key));
    }

    pub fn add(&mut self, region_id: u64, peer: &metapb::Peer, start_key: &[u8], end_key: &[u8]) {
        self.add_qps(region_id, peer, 1);
        self.add_key_range(region_id, start_key, end_key);
    }

    fn update(&mut self, other: &mut SplitHub) {
        for (region_id, region_info) in other.region_qps.iter() {
            self.add_qps(*region_id, &(*region_info).peer, (*region_info).qps);
        }
        for (region_id, other_key_ranges) in other.region_keys.iter_mut() {
            let key_ranges = self.region_keys.entry(*region_id).or_insert_with(|| vec![]);
            (*key_ranges).append(other_key_ranges);
        }
    }

    fn clear(&mut self) {
        self.region_keys.clear();
        self.region_qps.clear();
        self.region_recorder.retain(|_, recorder| {
            recorder.create_time.elapsed().unwrap() < DETECT_INTERVAL * DETECT_TIMES * 10
        });
    }

    pub fn flush(&mut self) -> (Vec<u32>, Vec<SplitInfo>) {
        let mut split_infos = Vec::default();
        let mut top = BinaryHeap::with_capacity(TOP_N as usize);
        for (region_id, region_info) in self.region_qps.iter() {
            let qps = (*region_info).qps;
            if qps > self.qps_threshold {
                let recorder = self
                    .region_recorder
                    .entry(*region_id)
                    .or_insert_with(Recorder::new);
                recorder.record(self.region_keys.get(region_id).unwrap());
                let key = recorder.split_key(self.split_score, self.min_sample_num);
                if !key.is_empty() {
                    let split_info = SplitInfo {
                        region_id: *region_id,
                        split_key: Key::from_raw(&key).into_encoded(),
                        peer: (*region_info).peer.clone(),
                    };
                    split_infos.push(split_info);
                    self.region_recorder.remove(region_id);
                    info!("load base split region";"region_id"=>*region_id);
                }
            } else {
                self.region_recorder.remove_entry(region_id);
            }
            top.push(qps);
        }
        self.clear();
        (top.into_vec(), split_infos)
    }
}

#[cfg(not(target_os = "macos"))]
#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Instant;
    use tikv_util::worker::FutureWorker;

    use super::*;

    struct RunnerTest {
        store_stat: Arc<Mutex<StoreStat>>,
        stats_monitor: StatsMonitor,
    }

    impl RunnerTest {
        fn new(
            interval: u64,
            scheduler: Scheduler<Task>,
            store_stat: Arc<Mutex<StoreStat>>,
        ) -> RunnerTest {
            let mut stats_monitor = StatsMonitor::new(Duration::from_secs(interval), scheduler);

            if let Err(e) = stats_monitor.start(SplitHubConfig::default()) {
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

    impl Runnable<Task> for RunnerTest {
        fn run(&mut self, task: Task, _handle: &Handle) {
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

    #[test]
    fn test_sample() {
        let mut recorder = Recorder::new();
        recorder.samples.push(Sample::new(b"c"));

        let key_range = KeyRange::new(b"a", b"b");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].left, 1);

        let key_range = KeyRange::new(b"a", b"c");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].left, 2);

        let key_range = KeyRange::new(b"a", b"d");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].contained, 1);

        let key_range = KeyRange::new(b"c", b"d");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].right, 1);

        let key_range = KeyRange::new(b"d", b"e");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].right, 2);

        let key_range = KeyRange::new(b"a", b"");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].left, 3);

        let key_range = KeyRange::new(b"c", b"");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].right, 3);

        let key_range = KeyRange::new(b"d", b"");
        recorder.sample(&key_range);
        assert_eq!(recorder.samples[0].right, 4);
    }

    #[test]
    fn test_recorder() {
        let mut recorder = Recorder::new();
        let mut key_ranges: Vec<KeyRange> = Vec::new();
        key_ranges.push(KeyRange::new(b"a", b"b"));
        key_ranges.push(KeyRange::new(b"b", b"c"));
        key_ranges.push(KeyRange::new(b"c", b"d"));
        key_ranges.push(KeyRange::new(b"d", b""));
        for _ in 0..50 {
            recorder.record(key_ranges.as_slice());
        }
        assert_eq!(recorder.samples.len(), 20);

        let split_key = recorder.split_key(DEFAULT_SPLIT_SCORE, MIN_SAMPLE_NUM);
        if split_key != b"c" {
            for sample_key in recorder.samples {
                assert_ne!(sample_key.key, b"c");
            }
        }
    }

    #[test]
    fn test_hub() {
        let mut hub = SplitHub::new();
        hub.interval = Duration::from_secs(65535); // avoid gc
        hub.qps_threshold = 1;
        hub.min_sample_num = 0;
        for i in 0..100 {
            for _ in 0..100 {
                hub.add(1, &metapb::Peer::default(), b"a", b"b");
                hub.add(1, &metapb::Peer::default(), b"b", b"c");
            }
            let (_, split_infos) = hub.flush();
            if (i + 1) % DETECT_TIMES == 0 {
                assert_eq!(split_infos.len(), 1);
                assert_eq!(
                    Key::from_encoded(split_infos[0].split_key.clone())
                        .into_raw()
                        .unwrap(),
                    b"b"
                );
            }
        }
    }
}
