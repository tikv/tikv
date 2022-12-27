// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{metapb, pdpb};
use pd_client::{metrics::PD_HEARTBEAT_COUNTER_VEC, PdClient, RegionStat};
use slog::{debug, info};
use tikv_util::{store::QueryStats, time::UnixSecs};

use super::{requests::*, Runner};

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
    // last_store_report_attributes records the state of the last store heartbeat
    pub last_store_report_read_bytes: u64,
    pub last_store_report_read_keys: u64,
    pub last_store_report_query_stats: QueryStats,
    pub approximate_keys: u64,
    pub approximate_size: u64,
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

        let peer_stat = self
            .region_peers
            .entry(region_id)
            .or_insert_with(PeerStat::default);
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

        // Calculate the CPU usage since the last region heartbeat.
        let cpu_usage = {
            // Take out the region CPU record.
            let cpu_time_duration = Duration::from_millis(
                self.region_cpu_records.remove(&region_id).unwrap_or(0) as u64,
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

        let region_stat = RegionStat {
            down_peers: task.down_peers,
            pending_peers: task.pending_peers,
            written_bytes: written_bytes_delta,
            written_keys: written_keys_delta,
            read_bytes: read_bytes_delta,
            read_keys: read_keys_delta,
            query_stats: query_stats.0,
            approximate_size,
            approximate_keys,
            last_report_ts,
            cpu_usage,
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
                        // TODO
                        info!(logger, "pd asks for split but ignored");
                    } else if resp.has_merge() {
                        // TODO
                        info!(logger, "pd asks for merge but ignored");
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
}
