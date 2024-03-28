// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, sync::Arc};

use collections::{HashMap, HashSet};
use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use health_controller::types::LatencyInspector;
use kvproto::pdpb;
use pd_client::{
    metrics::{
        REGION_READ_BYTES_HISTOGRAM, REGION_READ_KEYS_HISTOGRAM, REGION_WRITTEN_BYTES_HISTOGRAM,
        REGION_WRITTEN_KEYS_HISTOGRAM, STORE_SIZE_EVENT_INT_VEC,
    },
    PdClient,
};
use prometheus::local::LocalHistogram;
use raftstore::store::{
    metrics::STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC, UnsafeRecoveryExecutePlanSyncer,
    UnsafeRecoveryForceLeaderSyncer, UnsafeRecoveryHandle,
};
use slog::{error, info, warn};
use tikv_util::{
    metrics::RecordPairVec,
    store::QueryStats,
    time::{Duration, Instant as TiInstant, UnixSecs},
    topn::TopN,
};

use super::Runner;
use crate::router::{StoreMsg, UnsafeRecoveryRouter};

const HOTSPOT_REPORT_CAPACITY: usize = 1000;

/// Max limitation of delayed store heartbeat.
const STORE_HEARTBEAT_DELAY_LIMIT: u64 = Duration::from_secs(5 * 60).as_secs();

fn hotspot_key_report_threshold() -> u64 {
    const HOTSPOT_KEY_RATE_THRESHOLD: u64 = 128;
    fail_point!("mock_hotspot_threshold", |_| { 0 });
    HOTSPOT_KEY_RATE_THRESHOLD * 10
}

fn hotspot_byte_report_threshold() -> u64 {
    const HOTSPOT_BYTE_RATE_THRESHOLD: u64 = 8 * 1024;
    fail_point!("mock_hotspot_threshold", |_| { 0 });
    HOTSPOT_BYTE_RATE_THRESHOLD * 10
}

fn hotspot_query_num_report_threshold() -> u64 {
    const HOTSPOT_QUERY_RATE_THRESHOLD: u64 = 128;
    fail_point!("mock_hotspot_threshold", |_| { 0 });
    HOTSPOT_QUERY_RATE_THRESHOLD * 10
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

#[derive(Default, Clone)]
struct PeerCmpReadStat {
    pub region_id: u64,
    pub report_stat: u64,
}

impl Ord for PeerCmpReadStat {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
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
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.report_stat.cmp(&other.report_stat))
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

fn get_read_query_num(stat: &pdpb::QueryStats) -> u64 {
    stat.get_get() + stat.get_coprocessor() + stat.get_scan()
}

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn handle_store_heartbeat(
        &mut self,
        mut stats: pdpb::StoreStats,
        is_fake_hb: bool,
        store_report: Option<pdpb::StoreReport>,
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
        let (capacity, used_size, available) = self.collect_engine_size().unwrap_or_default();
        if available == 0 {
            warn!(self.logger, "no available space");
        }

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
        let last_query_sum = res.get_all_query_num();
        stats.set_query_stats(res.0);

        stats.set_cpu_usages(self.store_stat.store_cpu_usages.clone().into());
        stats.set_read_io_rates(self.store_stat.store_read_io_rates.clone().into());
        stats.set_write_io_rates(self.store_stat.store_write_io_rates.clone().into());
        // Update grpc server status
        stats.set_is_grpc_paused(self.grpc_service_manager.is_paused());

        let mut interval = pdpb::TimeInterval::default();
        interval.set_start_timestamp(self.store_stat.last_report_ts.into_inner());
        stats.set_interval(interval);
        self.store_stat.engine_last_total_bytes_read = self.store_stat.engine_total_bytes_read;
        self.store_stat.engine_last_total_keys_read = self.store_stat.engine_total_keys_read;
        self.store_stat
            .engine_last_query_num
            .fill_query_stats(&self.store_stat.engine_total_query_num);
        self.store_stat.last_report_ts = if is_fake_hb {
            // The given Task::StoreHeartbeat should be a fake heartbeat to PD, we won't
            // update the last_report_ts to avoid incorrectly marking current TiKV node in
            // normal state.
            self.store_stat.last_report_ts
        } else {
            UnixSecs::now()
        };
        self.store_stat.region_bytes_written.flush();
        self.store_stat.region_keys_written.flush();
        self.store_stat.region_bytes_read.flush();
        self.store_stat.region_keys_read.flush();

        STORE_SIZE_EVENT_INT_VEC.capacity.set(capacity as i64);
        STORE_SIZE_EVENT_INT_VEC.available.set(available as i64);
        STORE_SIZE_EVENT_INT_VEC.used.set(used_size as i64);

        // Update slowness statistics
        self.update_slowness_in_store_stats(&mut stats, last_query_sum);

        let resp = self.pd_client.store_heartbeat(stats, store_report, None);
        let logger = self.logger.clone();
        let router = self.router.clone();
        let mut grpc_service_manager = self.grpc_service_manager.clone();
        let f = async move {
            match resp.await {
                Ok(mut resp) => {
                    // TODO: handle replication_status

                    if let Some(mut plan) = resp.recovery_plan.take() {
                        let handle = Arc::new(UnsafeRecoveryRouter::new(router));
                        info!(logger, "Unsafe recovery, received a recovery plan");
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
                                    error!(logger,
                                        "fail to send force leader message for recovery";
                                        "err" => ?e);
                                }
                            }
                        } else {
                            let syncer = UnsafeRecoveryExecutePlanSyncer::new(
                                plan.get_step(),
                                handle.clone(),
                            );
                            for create in plan.take_creates().into_iter() {
                                if let Err(e) = handle.send_create_peer(create, syncer.clone()) {
                                    error!(logger,
                                        "fail to send create peer message for recovery";
                                        "err" => ?e);
                                }
                            }
                            for tombstone in plan.take_tombstones().into_iter() {
                                if let Err(e) = handle.send_destroy_peer(tombstone, syncer.clone())
                                {
                                    error!(logger,
                                        "fail to send destroy peer message for recovery";
                                        "err" => ?e);
                                }
                            }
                            for mut demote in plan.take_demotes().into_iter() {
                                if let Err(e) = handle.send_demote_peers(
                                    demote.get_region_id(),
                                    demote.take_failed_voters().into_vec(),
                                    syncer.clone(),
                                ) {
                                    error!(logger,
                                        "fail to send update peer list message for recovery";
                                        "err" => ?e);
                                }
                            }
                        }
                    }

                    // Attention, as Hibernate Region is eliminated in
                    // raftstore-v2, followings just mock the awaken
                    // operation.
                    if resp.awaken_regions.take().is_some() {
                        info!(
                            logger,
                            "Ignored AwakenRegions in raftstore-v2 as no hibernated regions in raftstore-v2"
                        );
                    }
                    // Control grpc server.
                    else if let Some(op) = resp.control_grpc.take() {
                        info!(logger, "forcely control grpc server";
                                "is_grpc_server_paused" => grpc_service_manager.is_paused(),
                                "event" => ?op,
                        );
                        match op.get_ctrl_event() {
                            pdpb::ControlGrpcEvent::Pause => {
                                if let Err(e) = grpc_service_manager.pause() {
                                    warn!(logger, "failed to send service event to PAUSE grpc server";
                                        "err" => ?e);
                                }
                            }
                            pdpb::ControlGrpcEvent::Resume => {
                                if let Err(e) = grpc_service_manager.resume() {
                                    warn!(logger, "failed to send service event to RESUME grpc server";
                                        "err" => ?e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(logger, "store heartbeat failed"; "err" => ?e);
                }
            }
        };
        self.remote.spawn(f);
    }

    /// Force to send a special heartbeat to pd when current store is hung on
    /// some special circumstances, i.e. disk busy, handler busy and others.
    pub fn handle_fake_store_heartbeat(&mut self) {
        let mut stats = pdpb::StoreStats::default();
        stats.set_store_id(self.store_id);
        stats.set_region_count(self.region_peers.len() as u32);

        let snap_stats = self.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

        // This calling means that the current node cannot report heartbeat in normaly
        // scheduler. That is, the current node must in `busy` state.
        stats.set_is_busy(true);

        // And here, the `is_fake_hb` should be marked with `True` to represent that
        // this heartbeat message is a fake one.
        self.handle_store_heartbeat(stats, true, None);
        warn!(self.logger, "scheduling store_heartbeat timeout, force report store slow score to pd.";
            "store_id" => self.store_id,
        );
    }

    pub fn is_store_heartbeat_delayed(&self) -> bool {
        let now = UnixSecs::now();
        let interval_second = now.into_inner() - self.store_stat.last_report_ts.into_inner();
        let store_heartbeat_interval = std::cmp::max(self.store_heartbeat_interval.as_secs(), 1);
        // Only if the `last_report_ts`, that is, the last timestamp of
        // store_heartbeat, exceeds the interval of store heartbaet but less than
        // the given limitation, will it trigger a report of fake heartbeat to
        // make the statistics of slowness percepted by PD timely.
        (interval_second > store_heartbeat_interval)
            && (interval_second <= STORE_HEARTBEAT_DELAY_LIMIT)
            && (interval_second % store_heartbeat_interval == 0)
    }

    pub fn handle_inspect_latency(&self, send_time: TiInstant, inspector: LatencyInspector) {
        let msg = StoreMsg::LatencyInspect {
            send_time,
            inspector,
        };
        if let Err(e) = self.router.send_control(msg) {
            warn!(self.logger, "pd worker send latency inspecter failed";
                    "err" => ?e);
        }
    }

    pub fn handle_update_store_infos(
        &mut self,
        cpu_usages: RecordPairVec,
        read_io_rates: RecordPairVec,
        write_io_rates: RecordPairVec,
    ) {
        self.store_stat.store_cpu_usages = cpu_usages;
        self.store_stat.store_read_io_rates = read_io_rates;
        self.store_stat.store_write_io_rates = write_io_rates;
    }

    /// Returns (capacity, used, available).
    fn collect_engine_size(&self) -> Option<(u64, u64, u64)> {
        let disk_stats = match fs2::statvfs(self.tablet_registry.tablet_root()) {
            Err(e) => {
                error!(
                    self.logger,
                    "get disk stat for rocksdb failed";
                    "engine_path" => self.tablet_registry.tablet_root().display(),
                    "err" => ?e
                );
                return None;
            }
            Ok(stats) => stats,
        };
        let disk_cap = disk_stats.total_space();
        let capacity = if self.cfg.value().capacity.0 == 0 {
            disk_cap
        } else {
            std::cmp::min(disk_cap, self.cfg.value().capacity.0)
        };
        let mut kv_size = 0;
        self.tablet_registry.for_each_opened_tablet(|_, cached| {
            if let Some(tablet) = cached.latest() {
                kv_size += tablet.get_engine_used_size().unwrap_or(0);
            }
            true
        });
        let snap_size = self.snap_mgr.total_snap_size().unwrap();
        let raft_size = self
            .raft_engine
            .get_engine_size()
            .expect("engine used size");

        STORE_SIZE_EVENT_INT_VEC.kv_size.set(kv_size as i64);
        STORE_SIZE_EVENT_INT_VEC.raft_size.set(raft_size as i64);
        STORE_SIZE_EVENT_INT_VEC.snap_size.set(snap_size as i64);

        let used_size = snap_size + kv_size + raft_size;
        let mut available = capacity.checked_sub(used_size).unwrap_or_default();
        // We only care about rocksdb SST file size, so we should check disk available
        // here.
        available = cmp::min(available, disk_stats.available_space());
        Some((capacity, used_size, available))
    }
}
