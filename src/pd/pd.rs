// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::fmt::{self, Display, Formatter};

use futures::Future;
use tokio_core::reactor::Handle;

use kvproto::metapb::{Peer, Region, RegionEpoch};
use raft::eraftpb::ConfChangeType;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::pdpb;
use rocksdb::DB;
use fs2;

use util::worker::FutureRunnable as Runnable;
use util::escape;
use util::transport::SendCh;
use util::rocksdb::*;
use raftstore::store::Msg;
use raftstore::store::util::{get_region_approximate_size, is_epoch_stale};
use raftstore::store::store::StoreInfo;
use raftstore::store::Callback;
use storage::FlowStatistics;
use util::collections::HashMap;
use prometheus::local::LocalHistogram;

use super::{PdClient, RegionStat};
use super::metrics::*;

// Use an asynchronous thread to tell pd something.
#[derive(Debug)]
pub enum Task {
    AskSplit {
        region: Region,
        split_key: Vec<u8>,
        peer: Peer,
        // If true, right region derive origin region_id.
        right_derive: bool,
        callback: Callback,
    },
    Heartbeat {
        region: Region,
        peer: Peer,
        down_peers: Vec<pdpb::PeerStats>,
        pending_peers: Vec<Peer>,
        total_written_bytes: u64,
        total_written_keys: u64,
        region_size: Option<u64>,
    },
    StoreHeartbeat {
        stats: pdpb::StoreStats,
        store_info: StoreInfo,
    },
    ReportSplit {
        left: Region,
        right: Region,
    },
    ValidatePeer {
        region: Region,
        peer: Peer,
        merge_source: Option<u64>,
    },
    ReadStats {
        read_stats: HashMap<u64, FlowStatistics>,
    },
    DestroyPeer {
        region_id: u64,
    },
}

pub struct StoreStat {
    pub engine_total_bytes_read: u64,
    pub engine_total_keys_read: u64,
    pub engine_last_total_bytes_read: u64,
    pub engine_last_total_keys_read: u64,

    pub region_bytes_read: LocalHistogram,
    pub region_keys_read: LocalHistogram,
    pub region_bytes_written: LocalHistogram,
    pub region_keys_written: LocalHistogram,
}

impl Default for StoreStat {
    fn default() -> StoreStat {
        StoreStat {
            region_bytes_read: REGION_READ_BYTES_HISTOGRAM.local(),
            region_keys_read: REGION_READ_KEYS_HISTOGRAM.local(),
            region_bytes_written: REGION_WRITTEN_BYTES_HISTOGRAM.local(),
            region_keys_written: REGION_WRITTEN_KEYS_HISTOGRAM.local(),

            engine_total_bytes_read: 0,
            engine_total_keys_read: 0,
            engine_last_total_bytes_read: 0,
            engine_last_total_keys_read: 0,
        }
    }
}

#[derive(Default, Debug)]
struct RegionHeartbeatCache {
    region: Region,
    peer: Peer,
    down_peers: Vec<pdpb::PeerStats>,
    pending_peers: Vec<Peer>,
    approximate_size: u64,

    read_bytes_delta: u64,
    read_keys_delta: u64,
    written_bytes_delta: u64,
    written_keys_delta: u64,

    last_written_bytes: u64,
    last_written_keys: u64,

    valid: bool,
}

impl RegionHeartbeatCache {
    fn heartbeat(&mut self) -> Option<(Region, Peer, RegionStat)> {
        if self.valid {
            None
        } else {
            self.valid = true;
            let region_stat = RegionStat::new(
                self.down_peers.clone(),
                self.pending_peers.clone(),
                self.written_bytes_delta,
                self.written_keys_delta,
                self.read_bytes_delta,
                self.read_keys_delta,
                self.approximate_size,
            );
            self.read_bytes_delta = 0;
            self.read_keys_delta = 0;
            self.written_bytes_delta = 0;
            self.written_keys_delta = 0;
            Some((self.region.clone(), self.peer.clone(), region_stat))
        }
    }

    fn merge_flow_statistics(&mut self, stats: FlowStatistics) {
        if stats.read_bytes > 0 {
            self.read_bytes_delta += stats.read_bytes as u64;
            self.valid = false;
        }
        if stats.read_keys > 0 {
            self.read_keys_delta += stats.read_keys as u64;
            self.valid = false;
        }
    }

    fn merge_heartbeat(&mut self, db: &DB, hb_task: Task) {
        match hb_task {
            Task::Heartbeat {
                region,
                peer,
                down_peers,
                pending_peers,
                total_written_bytes,
                total_written_keys,
                region_size,
            } => {
                let approximate_size = match region_size {
                    Some(size) => size,
                    None => get_region_approximate_size(db, &region).unwrap_or(0),
                };
                if self.approximate_size != approximate_size {
                    self.approximate_size = approximate_size;
                    self.valid = false;
                }

                if self.region != region {
                    self.region = region;
                    self.valid = false;
                }

                if self.peer != peer {
                    self.peer = peer;
                    self.valid = false;
                }

                if self.down_peers != down_peers {
                    self.down_peers = down_peers;
                    self.valid = false;
                }

                if self.pending_peers != pending_peers {
                    self.pending_peers = pending_peers;
                    self.valid = false;
                }

                if self.last_written_bytes < total_written_bytes {
                    self.written_bytes_delta += total_written_bytes - self.last_written_bytes;
                    self.last_written_bytes = total_written_bytes;
                    self.valid = false;
                }
                // TODO: what if last_written_bytes > total_written_bytes?

                if self.last_written_keys < total_written_keys {
                    self.written_keys_delta += total_written_keys - self.last_written_keys;
                    self.last_written_keys = total_written_keys;
                    self.valid = false;
                }
            }
            _ => unreachable!(),
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::AskSplit {
                ref region,
                ref split_key,
                ..
            } => write!(
                f,
                "ask split region {} with key {}",
                region.get_id(),
                escape(split_key)
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
            Task::ReportSplit {
                ref left,
                ref right,
            } => write!(f, "report split left {:?}, right {:?}", left, right),
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
            Task::DestroyPeer { ref region_id } => write!(f, "destroy peer {}", region_id),
        }
    }
}

pub struct Runner<T: PdClient> {
    store_id: u64,
    pd_client: Arc<T>,
    ch: SendCh<Msg>,
    db: Arc<DB>,
    store_stat: StoreStat,
    is_hb_receiver_scheduled: bool,
    is_handle_reconnect_scheduled: bool,

    invalidate_cache: Arc<AtomicBool>,
    heartbeat_cache: HashMap<u64, RegionHeartbeatCache>,
}

impl<T: PdClient> Runner<T> {
    pub fn new(store_id: u64, pd_client: Arc<T>, ch: SendCh<Msg>, db: Arc<DB>) -> Runner<T> {
        Runner {
            store_id: store_id,
            pd_client: pd_client,
            ch: ch,
            db: db,
            is_hb_receiver_scheduled: false,
            is_handle_reconnect_scheduled: false,
            store_stat: StoreStat::default(),
            invalidate_cache: Arc::new(AtomicBool::new(false)),
            heartbeat_cache: HashMap::default(),
        }
    }

    fn handle_ask_split(
        &self,
        handle: &Handle,
        mut region: Region,
        split_key: Vec<u8>,
        peer: Peer,
        right_derive: bool,
        callback: Callback,
    ) {
        let ch = self.ch.clone();
        let f = self.pd_client.ask_split(region.clone()).then(move |resp| {
            match resp {
                Ok(mut resp) => {
                    info!(
                        "[region {}] try to split with new region id {} for region {:?}",
                        region.get_id(),
                        resp.get_new_region_id(),
                        region
                    );

                    let req = new_split_region_request(
                        split_key,
                        resp.get_new_region_id(),
                        resp.take_new_peer_ids(),
                        right_derive,
                    );
                    let region_id = region.get_id();
                    let epoch = region.take_region_epoch();
                    send_admin_request(&ch, region_id, epoch, peer, req, callback)
                }
                Err(e) => {
                    debug!("[region {}] failed to ask split: {:?}", region.get_id(), e);
                }
            }
            Ok(())
        });
        handle.spawn(f)
    }

    fn handle_heartbeat(&mut self, handle: &Handle, region_id: u64, hb_task: Task) {
        let cache = self.heartbeat_cache
            .entry(region_id)
            .or_insert_with(RegionHeartbeatCache::default);
        cache.merge_heartbeat(&self.db, hb_task);

        self.store_stat
            .region_bytes_written
            .observe(cache.written_bytes_delta as f64);
        self.store_stat
            .region_keys_written
            .observe(cache.written_keys_delta as f64);
        self.store_stat
            .region_bytes_read
            .observe(cache.read_bytes_delta as f64);
        self.store_stat
            .region_keys_read
            .observe(cache.read_keys_delta as f64);

        if let Some((region, peer, region_stat)) = cache.heartbeat() {
            debug!("[region {}] send changed heartbeat to pd", region_id);
            let f = self.pd_client
                .region_heartbeat(region, peer, region_stat)
                .map_err(move |e| {
                    debug!("[region {}] failed to send heartbeat: {:?}", region_id, e);
                });
            handle.spawn(f);
        }
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
                    "get disk stat for rocksdb {} failed: {}",
                    store_info.engine.path(),
                    e
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

        let used_size =
            stats.get_used_size() + get_engine_used_size(Arc::clone(&store_info.engine));
        stats.set_used_size(used_size);

        let mut available = if capacity > used_size {
            capacity - used_size
        } else {
            warn!("no available space");
            0
        };

        // We only care rocksdb SST file size, so we should
        // check disk available here.
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
        self.store_stat.engine_last_total_bytes_read = self.store_stat.engine_total_bytes_read;
        self.store_stat.engine_last_total_keys_read = self.store_stat.engine_total_keys_read;

        self.store_stat.region_bytes_written.flush();
        self.store_stat.region_keys_written.flush();
        self.store_stat.region_bytes_read.flush();
        self.store_stat.region_keys_read.flush();

        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["capacity"])
            .set(capacity as f64);
        STORE_SIZE_GAUGE_VEC
            .with_label_values(&["available"])
            .set(available as f64);

        let f = self.pd_client.store_heartbeat(stats).map_err(|e| {
            error!("store heartbeat failed {:?}", e);
        });
        handle.spawn(f);
    }

    fn handle_report_split(&self, handle: &Handle, left: Region, right: Region) {
        let f = self.pd_client.report_split(left, right).map_err(|e| {
            debug!("report split failed {:?}", e);
        });
        handle.spawn(f);
    }

    fn handle_validate_peer(
        &self,
        handle: &Handle,
        local_region: Region,
        peer: Peer,
        merge_source: Option<u64>,
    ) {
        let ch = self.ch.clone();
        let f = self.pd_client
            .get_region_by_id(local_region.get_id())
            .then(move |resp| {
                match resp {
                    Ok(Some(pd_region)) => {
                        if is_epoch_stale(
                            pd_region.get_region_epoch(),
                            local_region.get_region_epoch(),
                        ) {
                            // The local region epoch is fresher than region epoch in PD
                            // This means the region info in PD is not updated to the latest even
                            // after max_leader_missing_duration. Something is wrong in the system.
                            // Just add a log here for this situation.
                            info!(
                                "[region {}] {} the local region epoch: {:?} is greater the \
                                 region epoch in PD: {:?}, ignored.",
                                local_region.get_id(),
                                peer.get_id(),
                                local_region.get_region_epoch(),
                                pd_region.get_region_epoch()
                            );
                            PD_VALIDATE_PEER_COUNTER_VEC
                                .with_label_values(&["region epoch error"])
                                .inc();
                            return Ok(());
                        }

                        if pd_region.get_peers().into_iter().all(|p| p != &peer) {
                            // Peer is not a member of this region anymore. Probably it's removed out.
                            // Send it a raft massage to destroy it since it's obsolete.
                            info!(
                                "[region {}] {} is not a valid member of region {:?}. To be \
                                 destroyed soon.",
                                local_region.get_id(),
                                peer.get_id(),
                                pd_region
                            );
                            PD_VALIDATE_PEER_COUNTER_VEC
                                .with_label_values(&["peer stale"])
                                .inc();
                            if let Some(source) = merge_source {
                                send_merge_fail(ch, source);
                            } else {
                                send_destroy_peer_message(ch, local_region, peer, pd_region);
                            }
                            return Ok(());
                        }
                        info!(
                            "[region {}] {} is still valid in region {:?}",
                            local_region.get_id(),
                            peer.get_id(),
                            pd_region
                        );
                        PD_VALIDATE_PEER_COUNTER_VEC
                            .with_label_values(&["peer valid"])
                            .inc();
                    }
                    Ok(None) => {
                        // splitted region has not yet reported to pd.
                        // TODO: handle merge
                    }
                    Err(e) => {
                        error!("get region failed {:?}", e);
                    }
                }
                Ok(())
            });
        handle.spawn(f);
    }

    fn schedule_heartbeat_receiver(&mut self, handle: &Handle) {
        let ch = self.ch.clone();
        let store_id = self.store_id;
        let f = self.pd_client
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
                        "[region {}] try to change peer {:?} {:?}",
                        region_id,
                        change_peer.get_change_type(),
                        change_peer.get_peer()
                    );
                    let req = new_change_peer_request(
                        change_peer.get_change_type(),
                        change_peer.take_peer(),
                    );
                    send_admin_request(&ch, region_id, epoch, peer, req, Callback::None);
                } else if resp.has_transfer_leader() {
                    PD_HEARTBEAT_COUNTER_VEC
                        .with_label_values(&["transfer leader"])
                        .inc();

                    let mut transfer_leader = resp.take_transfer_leader();
                    info!(
                        "[region {}] try to transfer leader from {:?} to {:?}",
                        region_id,
                        peer,
                        transfer_leader.get_peer()
                    );
                    let req = new_transfer_leader_request(transfer_leader.take_peer());
                    send_admin_request(&ch, region_id, epoch, peer, req, Callback::None)
                } else if resp.has_merge() {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["merge"]).inc();

                    let merge = resp.take_merge();
                    info!("[region {}] try to merge {:?}", region_id, merge);
                    let req = new_merge_request(merge);
                    send_admin_request(&ch, region_id, epoch, peer, req, Callback::None)
                } else {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["noop"]).inc();
                }
            })
            .map_err(|e| panic!("unexpected error: {:?}", e))
            .map(move |_| {
                info!(
                    "[store {}] region heartbeat response handler exit.",
                    store_id
                )
            });
        handle.spawn(f);
        self.is_hb_receiver_scheduled = true;
    }

    fn handle_reconnect(&mut self) {
        if self.is_handle_reconnect_scheduled {
            if self.invalidate_cache.swap(false, Ordering::Relaxed) {
                info!("invalidate region heartbeat cache");
                self.heartbeat_cache.clear();
            }
        } else {
            self.is_handle_reconnect_scheduled = true;
            let invalidate_cache = Arc::clone(&self.invalidate_cache);
            self.pd_client
                .handle_reconnect(move || invalidate_cache.store(true, Ordering::Relaxed));
        }
    }

    fn handle_read_stats(&mut self, read_stats: HashMap<u64, FlowStatistics>) {
        for (region_id, stats) in read_stats {
            self.store_stat.engine_total_bytes_read += stats.read_bytes as u64;
            self.store_stat.engine_total_keys_read += stats.read_keys as u64;

            let cache = self.heartbeat_cache
                .entry(region_id)
                .or_insert_with(RegionHeartbeatCache::default);
            cache.merge_flow_statistics(stats)
        }
    }

    fn handle_destroy_peer(&mut self, region_id: u64) {
        match self.heartbeat_cache.remove(&region_id) {
            None => return,
            Some(_) => info!("[region {}] remove peer statistic record in pd", region_id),
        }
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task, handle: &Handle) {
        debug!("executing task {}", task);

        if !self.is_hb_receiver_scheduled {
            self.schedule_heartbeat_receiver(handle);
        }
        self.handle_reconnect();

        match task {
            Task::AskSplit {
                region,
                split_key,
                peer,
                right_derive,
                callback,
            } => self.handle_ask_split(handle, region, split_key, peer, right_derive, callback),
            Task::Heartbeat {
                region,
                peer,
                down_peers,
                pending_peers,
                total_written_bytes,
                total_written_keys,
                region_size,
            } => {
                let region_id = region.get_id();
                self.handle_heartbeat(
                    handle,
                    region_id,
                    Task::Heartbeat {
                        region,
                        peer,
                        down_peers,
                        pending_peers,
                        total_written_bytes,
                        total_written_keys,
                        region_size,
                    },
                )
            }
            Task::StoreHeartbeat { stats, store_info } => {
                self.handle_store_heartbeat(handle, stats, store_info)
            }
            Task::ReportSplit { left, right } => self.handle_report_split(handle, left, right),
            Task::ValidatePeer {
                region,
                peer,
                merge_source,
            } => self.handle_validate_peer(handle, region, peer, merge_source),
            Task::ReadStats { read_stats } => self.handle_read_stats(read_stats),
            Task::DestroyPeer { region_id } => self.handle_destroy_peer(region_id),
        };
    }
}

fn new_change_peer_request(change_type: ConfChangeType, peer: Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
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
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_region_id(new_region_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req.mut_split().set_right_derive(right_derive);
    req
}

fn new_transfer_leader_request(peer: Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
    req
}

fn new_merge_request(merge: pdpb::Merge) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::PrepareMerge);
    req.mut_prepare_merge()
        .set_target(merge.get_target().to_owned());
    req
}

fn send_admin_request(
    ch: &SendCh<Msg>,
    region_id: u64,
    epoch: RegionEpoch,
    peer: Peer,
    request: AdminRequest,
    callback: Callback,
) {
    let cmd_type = request.get_cmd_type();

    let mut req = RaftCmdRequest::new();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_region_epoch(epoch);
    req.mut_header().set_peer(peer);

    req.set_admin_request(request);

    if let Err(e) = ch.try_send(Msg::new_raft_cmd(req, callback)) {
        error!(
            "[region {}] send {:?} request err {:?}",
            region_id, cmd_type, e
        );
    }
}

// send merge fail to gc merge source.
fn send_merge_fail(ch: SendCh<Msg>, source: u64) {
    if let Err(e) = ch.send(Msg::MergeFail { region_id: source }) {
        error!("[region {}] failed to report merge fail: {:?}", source, e);
    }
}

// send a raft message to destroy the specified stale peer
fn send_destroy_peer_message(ch: SendCh<Msg>, local_region: Region, peer: Peer, pd_region: Region) {
    let mut message = RaftMessage::new();
    message.set_region_id(local_region.get_id());
    message.set_from_peer(peer.clone());
    message.set_to_peer(peer.clone());
    message.set_region_epoch(pd_region.get_region_epoch().clone());
    message.set_is_tombstone(true);
    if let Err(e) = ch.try_send(Msg::RaftMessage(message)) {
        error!(
            "send gc peer request to region {} err {:?}",
            local_region.get_id(),
            e
        )
    }
}
