// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{metapb, pdpb};
use pd_client::{
    BucketStat, PdClient, RegionStat, RegionWriteCfCopDetail, metrics::PD_HEARTBEAT_COUNTER_VEC,
};
use raftstore::store::{ReadStats, WriteStats};
use resource_metering::RawRecords;
use slog::{debug, error, info};
use tikv_util::{store::QueryStats, time::UnixSecs};

use super::{Runner, requests::*};
use crate::{
    operation::{RequestHalfSplit, RequestSplit},
    router::{CmdResChannel, PeerMsg},
};

pub struct RegionHeartbeatTask {
    pub term: u64,
    pub region: metapb::Region,
    pub peer: metapb::Peer,
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub approximate_size: Option<u64>,
    pub approximate_keys: Option<u64>,
    pub wait_data_peers: Vec<u64>,
    // TODO: RegionReplicationStatus
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
    pub last_region_report_term: u64,
    pub last_region_report_leader_id: u64,
    pub last_region_report_leader_store_id: u64,
    pub last_region_report_region_version: u64,
    pub last_region_report_region_conf_ver: u64,
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

    fn report(&mut self) -> BucketStat {
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

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn handle_region_heartbeat(&mut self, task: RegionHeartbeatTask) {
        // HACK! In order to keep the compatible of protos, we use 0 to identify
        // the size uninitialized regions, and use 1 to identify the empty regions.
        //
        // See tikv/tikv#11114 for details.
        let approximate_size = match task.approximate_size {
            Some(0) => 1,
            Some(v) => v,
            None => 0, // size uninitialized
        };
        let approximate_keys = task.approximate_keys.unwrap_or_default();
        let region_id = task.region.get_id();
        let suppress_interval_secs = self.region_heartbeat_report_interval.as_secs();
        if suppress_interval_secs > 0 {
            let now = UnixSecs::now();
            let should_suppress = if let Some(peer_stat) = self.region_peers.get(&region_id) {
                let read_bytes_delta = peer_stat.read_bytes - peer_stat.last_region_report_read_bytes;
                let read_keys_delta = peer_stat.read_keys - peer_stat.last_region_report_read_keys;
                let written_bytes_delta =
                    task.written_bytes - peer_stat.last_region_report_written_bytes;
                let written_keys_delta = task.written_keys - peer_stat.last_region_report_written_keys;
                let query_stats = peer_stat
                    .query_stats
                    .sub_query_stats(&peer_stat.last_region_report_query_stats);
                let mut last_report_ts = peer_stat.last_region_report_ts;
                if last_report_ts.is_zero() {
                    last_report_ts = self.start_ts;
                }
                let interval_second = now
                    .into_inner()
                    .saturating_sub(last_report_ts.into_inner());
                let cpu_usage = {
                    let cpu_time_duration = Duration::from_millis(
                        self.region_cpu_records_since_region_heartbeat
                            .get(&region_id)
                            .copied()
                            .unwrap_or(0) as u64,
                    );
                    if interval_second > 0 {
                        ((cpu_time_duration.as_secs_f64() * 100.0) / interval_second as f64) as u64
                    } else {
                        0
                    }
                };
                let has_heartbeat_delta = read_bytes_delta > 0
                    || read_keys_delta > 0
                    || written_bytes_delta > 0
                    || written_keys_delta > 0
                    || query_stats.get_all_query_num() > 0
                    || cpu_usage > 0
                    || !task.down_peers.is_empty()
                    || !task.pending_peers.is_empty()
                    || !task.wait_data_peers.is_empty();
                should_suppress_redundant_region_heartbeat(
                    suppress_interval_secs,
                    now,
                    peer_stat,
                    &task,
                    approximate_size,
                    approximate_keys,
                    has_heartbeat_delta,
                )
            } else {
                false
            };
            if should_suppress {
                debug!(
                    self.logger,
                    "suppress redundant region heartbeat";
                    "region_id" => region_id,
                    "peer_id" => task.peer.get_id(),
                );
                return;
            }
        }

        let peer_stat = self.region_peers.entry(region_id).or_default();
        peer_stat.approximate_size = approximate_size;
        peer_stat.approximate_keys = approximate_keys;

        let read_bytes_delta = peer_stat.read_bytes - peer_stat.last_region_report_read_bytes;
        let read_keys_delta = peer_stat.read_keys - peer_stat.last_region_report_read_keys;
        let written_bytes_delta = task.written_bytes - peer_stat.last_region_report_written_bytes;
        let written_keys_delta = task.written_keys - peer_stat.last_region_report_written_keys;
        let query_stats = peer_stat
            .query_stats
            .sub_query_stats(&peer_stat.last_region_report_query_stats);
        let mut last_report_ts = peer_stat.last_region_report_ts;
        if last_report_ts.is_zero() {
            last_report_ts = self.start_ts;
        }
        peer_stat.last_region_report_written_bytes = task.written_bytes;
        peer_stat.last_region_report_written_keys = task.written_keys;
        peer_stat.last_region_report_read_bytes = peer_stat.read_bytes;
        peer_stat.last_region_report_read_keys = peer_stat.read_keys;
        peer_stat.last_region_report_query_stats = peer_stat.query_stats.clone();
        let unix_secs_now = UnixSecs::now();
        peer_stat.last_region_report_ts = unix_secs_now;
        peer_stat.last_region_report_term = task.term;
        peer_stat.last_region_report_leader_id = task.peer.get_id();
        peer_stat.last_region_report_leader_store_id = task.peer.get_store_id();
        peer_stat.last_region_report_region_version = task.region.get_region_epoch().get_version();
        peer_stat.last_region_report_region_conf_ver = task.region.get_region_epoch().get_conf_ver();

        // Calculate the CPU usage since the last region heartbeat.
        let cpu_usage = {
            // Take out the region CPU record.
            let cpu_time_duration = Duration::from_millis(
                self.region_cpu_records_since_region_heartbeat
                    .remove(&region_id)
                    .unwrap_or(0) as u64,
            );
            let interval_second = unix_secs_now.into_inner() - last_report_ts.into_inner();
            // Keep consistent with the calculation of cpu_usages in a store heartbeat.
            // See components/tikv_util/src/metrics/threads_linux.rs for more details.
            if interval_second > 0 {
                ((cpu_time_duration.as_secs_f64() * 100.0) / interval_second as f64) as u64
            } else {
                0
            }
        };
        let mut cpu_stats = pdpb::CpuStats::default();
        cpu_stats.set_unified_read(cpu_usage);

        let region_stat = RegionStat {
            down_peers: task.down_peers,
            pending_peers: task.pending_peers,
            written_bytes: written_bytes_delta,
            written_keys: written_keys_delta,
            read_bytes: read_bytes_delta,
            read_keys: read_keys_delta,
            query_stats: query_stats.0,
            cop_detail: RegionWriteCfCopDetail::default(),
            approximate_size,
            approximate_keys,
            last_report_ts,
            cpu_usage,
            cpu_stats,
        };
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
            task.term,
            task.region.clone(),
            task.peer,
            region_stat,
            None,
        );
        let logger = self.logger.clone();
        let f = async move {
            if let Err(e) = resp.await {
                debug!(
                    logger,
                    "failed to send heartbeat";
                    "region_id" => task.region.get_id(),
                    "err" => ?e
                );
            }
        };
        self.remote.spawn(f);
    }

    pub fn maybe_schedule_heartbeat_receiver(&mut self) {
        if self.is_hb_receiver_scheduled {
            return;
        }
        let router = self.router.clone();
        let store_id = self.store_id;
        let logger = self.logger.clone();

        let fut =
            self.pd_client
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
                            logger,
                            "try to change peer";
                            "region_id" => region_id,
                            "change_type" => ?change_peer.get_change_type(),
                            "peer" => ?change_peer.get_peer()
                        );
                        let req = new_change_peer_request(
                            change_peer.get_change_type(),
                            change_peer.take_peer(),
                        );
                        send_admin_request(&logger, &router, region_id, epoch, peer, req, None);
                    } else if resp.has_change_peer_v2() {
                        PD_HEARTBEAT_COUNTER_VEC
                            .with_label_values(&["change peer"])
                            .inc();

                        let mut change_peer_v2 = resp.take_change_peer_v2();
                        info!(
                            logger,
                            "try to change peer";
                            "region_id" => region_id,
                            "changes" => ?change_peer_v2.get_changes(),
                        );
                        let req = new_change_peer_v2_request(change_peer_v2.take_changes().into());
                        send_admin_request(&logger, &router, region_id, epoch, peer, req, None);
                    } else if resp.has_transfer_leader() {
                        PD_HEARTBEAT_COUNTER_VEC
                            .with_label_values(&["transfer leader"])
                            .inc();

                        let mut transfer_leader = resp.take_transfer_leader();
                        info!(
                            logger,
                            "try to transfer leader";
                            "region_id" => region_id,
                            "from_peer" => ?peer,
                            "to_peer" => ?transfer_leader.get_peer(),
                            "to_peers" => ?transfer_leader.get_peers(),
                        );
                        let req = new_transfer_leader_request(
                            transfer_leader.take_peer(),
                            transfer_leader.take_peers().into(),
                        );
                        send_admin_request(&logger, &router, region_id, epoch, peer, req, None);
                    } else if resp.has_split_region() {
                        PD_HEARTBEAT_COUNTER_VEC
                            .with_label_values(&["split region"])
                            .inc();

                        let mut split_region = resp.take_split_region();
                        info!(
                            logger,
                            "try to split";
                            "region_id" => region_id,
                            "region_epoch" => ?epoch,
                        );

                        let (ch, _) = CmdResChannel::pair();
                        let msg = if split_region.get_policy() == pdpb::CheckPolicy::Usekey {
                            PeerMsg::RequestSplit {
                                request: RequestSplit {
                                    epoch,
                                    split_keys: split_region.take_keys().into(),
                                    source: "pd".into(),
                                    share_source_region_size: false,
                                },
                                ch,
                            }
                        } else {
                            PeerMsg::RequestHalfSplit {
                                request: RequestHalfSplit {
                                    epoch,
                                    start_key: None,
                                    end_key: None,
                                    policy: split_region.get_policy(),
                                    source: "pd".into(),
                                },
                                ch,
                            }
                        };
                        if let Err(e) = router.send(region_id, msg) {
                            error!(logger,
                                "send split request failed";
                                "region_id" => region_id,
                                "err" => ?e
                            );
                        }
                    } else if resp.has_merge() {
                        PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["merge"]).inc();

                        let merge = resp.take_merge();
                        info!(logger, "try to merge"; "region_id" => region_id, "merge" => ?merge);
                        let req = new_merge_request(merge);
                        send_admin_request(&logger, &router, region_id, epoch, peer, req, None);
                    } else {
                        PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["noop"]).inc();
                    }
                });
        let logger = self.logger.clone();
        let f = async move {
            match fut.await {
                Ok(_) => {
                    info!(
                        logger,
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

    pub fn handle_report_region_buckets(&mut self, delta_buckets: BucketStat) {
        let region_id = delta_buckets.meta.region_id;
        self.merge_buckets(delta_buckets);
        let report_buckets = self.region_buckets.get_mut(&region_id).unwrap();
        let last_report_ts = if report_buckets.last_report_ts.is_zero() {
            self.start_ts
        } else {
            report_buckets.last_report_ts
        };
        let now = UnixSecs::now();
        let interval_second = now.into_inner() - last_report_ts.into_inner();
        report_buckets.last_report_ts = now;
        let delta = report_buckets.report();
        let resp = self
            .pd_client
            .report_region_buckets(&delta, Duration::from_secs(interval_second));
        let logger = self.logger.clone();
        let f = async move {
            if let Err(e) = resp.await {
                debug!(
                    logger,
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

    pub fn handle_update_read_stats(&mut self, mut stats: ReadStats) {
        for (region_id, region_info) in stats.region_infos.iter_mut() {
            let peer_stat = self.region_peers.entry(*region_id).or_default();
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
        for (_, delta_buckets) in std::mem::take(&mut stats.region_buckets) {
            self.merge_buckets(delta_buckets);
        }
        if !stats.region_infos.is_empty() {
            self.stats_monitor.maybe_send_read_stats(stats);
        }
    }

    pub fn handle_update_write_stats(&mut self, mut stats: WriteStats) {
        for (region_id, region_info) in stats.region_infos.iter_mut() {
            let peer_stat = self.region_peers.entry(*region_id).or_default();
            peer_stat.query_stats.add_query_stats(&region_info.0);
            self.store_stat
                .engine_total_query_num
                .add_query_stats(&region_info.0);
        }
    }

    pub fn handle_update_region_cpu_records(&mut self, records: Arc<RawRecords>) {
        // Send Region CPU info to AutoSplitController inside the stats_monitor.
        self.stats_monitor.maybe_send_cpu_stats(&records);
        Self::calculate_region_cpu_records(
            self.store_id,
            records.clone(),
            &mut self.region_cpu_records_since_region_heartbeat,
        );
        Self::calculate_region_cpu_records(
            self.store_id,
            records,
            &mut self.region_cpu_records_since_store_heartbeat,
        );
    }

    pub fn handle_destroy_peer(&mut self, region_id: u64) {
        let removed = remove_peer_stat_from_maps(
            region_id,
            &mut self.region_peers,
            &mut self.region_cpu_records_since_region_heartbeat,
            &mut self.region_cpu_records_since_store_heartbeat,
        );
        if removed {
            info!(self.logger, "remove peer statistic record in pd"; "region_id" => region_id);
        }
    }

    fn merge_buckets(&mut self, mut delta: BucketStat) {
        let region_id = delta.meta.region_id;
        self.region_buckets
            .entry(region_id)
            .and_modify(|report_bucket| {
                let current = &mut report_bucket.current_stat;
                if current.meta < delta.meta {
                    std::mem::swap(current, &mut delta);
                }
                current.merge(&delta);
            })
            .or_insert_with(|| ReportBucket::new(delta));
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
}

fn remove_peer_stat_from_maps(
    region_id: u64,
    region_peers: &mut HashMap<u64, PeerStat>,
    region_cpu_records_since_region_heartbeat: &mut HashMap<u64, u32>,
    region_cpu_records_since_store_heartbeat: &mut HashMap<u64, u32>,
) -> bool {
    let removed = region_peers.remove(&region_id).is_some();
    region_cpu_records_since_region_heartbeat.remove(&region_id);
    region_cpu_records_since_store_heartbeat.remove(&region_id);
    removed
}

fn should_suppress_redundant_region_heartbeat(
    suppress_interval_secs: u64,
    now: UnixSecs,
    peer_stat: &PeerStat,
    hb_task: &RegionHeartbeatTask,
    approximate_size: u64,
    approximate_keys: u64,
    has_heartbeat_delta: bool,
) -> bool {
    if suppress_interval_secs == 0 || has_heartbeat_delta || peer_stat.last_region_report_ts.is_zero() {
        return false;
    }
    let elapsed = now
        .into_inner()
        .saturating_sub(peer_stat.last_region_report_ts.into_inner());
    if elapsed >= suppress_interval_secs {
        return false;
    }
    if hb_task.term != peer_stat.last_region_report_term {
        return false;
    }
    if hb_task.peer.get_id() != peer_stat.last_region_report_leader_id
        || hb_task.peer.get_store_id() != peer_stat.last_region_report_leader_store_id
    {
        return false;
    }
    let epoch = hb_task.region.get_region_epoch();
    if epoch.get_version() != peer_stat.last_region_report_region_version
        || epoch.get_conf_ver() != peer_stat.last_region_report_region_conf_ver
    {
        return false;
    }
    if approximate_size != peer_stat.approximate_size || approximate_keys != peer_stat.approximate_keys
    {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_peer_stat_from_maps() {
        let mut region_peers = HashMap::default();
        region_peers.insert(1, PeerStat::default());
        let mut region_cpu_records_since_region_heartbeat = HashMap::default();
        region_cpu_records_since_region_heartbeat.insert(1, 10);
        let mut region_cpu_records_since_store_heartbeat = HashMap::default();
        region_cpu_records_since_store_heartbeat.insert(1, 12);

        assert!(remove_peer_stat_from_maps(
            1,
            &mut region_peers,
            &mut region_cpu_records_since_region_heartbeat,
            &mut region_cpu_records_since_store_heartbeat,
        ));
        assert!(region_peers.is_empty());
        assert!(region_cpu_records_since_region_heartbeat.is_empty());
        assert!(region_cpu_records_since_store_heartbeat.is_empty());
    }

    #[test]
    fn test_should_suppress_redundant_region_heartbeat() {
        let mut region = metapb::Region::default();
        region.set_id(1);
        let mut epoch = metapb::RegionEpoch::default();
        epoch.set_version(10);
        epoch.set_conf_ver(20);
        region.set_region_epoch(epoch);

        let mut peer = metapb::Peer::default();
        peer.set_id(11);
        peer.set_store_id(12);

        let now = UnixSecs::now();
        let mut peer_stat = PeerStat::default();
        peer_stat.last_region_report_ts = now;
        peer_stat.last_region_report_term = 3;
        peer_stat.last_region_report_leader_id = 11;
        peer_stat.last_region_report_leader_store_id = 12;
        peer_stat.last_region_report_region_version = 10;
        peer_stat.last_region_report_region_conf_ver = 20;
        peer_stat.approximate_size = 100;
        peer_stat.approximate_keys = 200;

        let hb_task = RegionHeartbeatTask {
            term: 3,
            region,
            peer,
            down_peers: vec![],
            pending_peers: vec![],
            written_bytes: 0,
            written_keys: 0,
            approximate_size: Some(100),
            approximate_keys: Some(200),
            wait_data_peers: vec![],
        };

        assert!(should_suppress_redundant_region_heartbeat(
            60,
            now,
            &peer_stat,
            &hb_task,
            100,
            200,
            false,
        ));
    }

    #[test]
    fn test_should_not_suppress_redundant_region_heartbeat_on_delta_or_metadata_change() {
        let mut region = metapb::Region::default();
        region.set_id(1);
        let mut epoch = metapb::RegionEpoch::default();
        epoch.set_version(10);
        epoch.set_conf_ver(20);
        region.set_region_epoch(epoch);

        let mut peer = metapb::Peer::default();
        peer.set_id(11);
        peer.set_store_id(12);

        let now = UnixSecs::now();
        let mut peer_stat = PeerStat::default();
        peer_stat.last_region_report_ts = now;
        peer_stat.last_region_report_term = 3;
        peer_stat.last_region_report_leader_id = 11;
        peer_stat.last_region_report_leader_store_id = 12;
        peer_stat.last_region_report_region_version = 10;
        peer_stat.last_region_report_region_conf_ver = 20;
        peer_stat.approximate_size = 100;
        peer_stat.approximate_keys = 200;

        let hb_task = RegionHeartbeatTask {
            term: 4,
            region,
            peer,
            down_peers: vec![],
            pending_peers: vec![],
            written_bytes: 0,
            written_keys: 0,
            approximate_size: Some(100),
            approximate_keys: Some(200),
            wait_data_peers: vec![],
        };

        assert!(!should_suppress_redundant_region_heartbeat(
            60,
            now,
            &peer_stat,
            &hb_task,
            100,
            200,
            false,
        ));
        assert!(!should_suppress_redundant_region_heartbeat(
            60,
            now,
            &peer_stat,
            &hb_task,
            100,
            200,
            true,
        ));
    }
}
