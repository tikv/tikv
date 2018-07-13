// Copyright 2018 PingCAP, Inc.
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

use futures_cpupool::CpuPool;
use import::SSTImporter;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState};
use raft::eraftpb::MessageType;
use raft::INVALID_INDEX;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::errors::Error;
use raftstore::store::engine::Peekable;
use raftstore::store::keys::{data_end_key, data_key, enc_start_key};
use raftstore::store::metrics::*;
use raftstore::store::peer::PeerHolder;
use raftstore::store::worker::{ApplyTask, RegionTask};
use raftstore::store::{keys, util, Config, Engines, Msg, Peer, StoreMeta};
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::mpsc::{SendError, TrySendError};
use std::sync::{Arc, Mutex};
use storage::CF_RAFT;
use util::collections::HashMap;
use util::mpsc::LooseBoundedSender;
use util::worker::{Scheduler, Worker};

struct PlaceHolder;

impl PlaceHolder {
    fn send(&self, _: RaftMessage) -> Result<(), Error> {
        Ok(())
    }
}

pub struct RangeState {
    region_ranges: BTreeMap<Vec<u8>, u64>,
    region_peers: HashMap<u64, Region>,
}

pub struct PeerMeta {
    pub region: Region,
    pub peer: metapb::Peer,
}

struct Meta {
    store_id: u64,
    cfg: Arc<Config>,
    engines: Engines,
    region_worker: Worker<RegionTask>,
    apply_worker: Worker<ApplyTask>,
    coprocessor_host: Arc<CoprocessorHost>,
    importer: Arc<SSTImporter>,
}

impl StoreMeta for Meta {
    #[inline]
    fn engines(&self) -> Engines {
        self.engines.clone()
    }

    #[inline]
    fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    #[inline]
    fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply_worker.scheduler()
    }

    #[inline]
    fn store_id(&self) -> u64 {
        self.store_id
    }

    #[inline]
    fn config(&self) -> Arc<Config> {
        Arc::clone(&self.cfg)
    }

    #[inline]
    fn coprocessor_host(&self) -> Arc<CoprocessorHost> {
        Arc::clone(&self.coprocessor_host)
    }

    #[inline]
    fn importer(&self) -> Arc<SSTImporter> {
        Arc::clone(&self.importer)
    }
}

pub struct StoreState {
    meta: Meta,
    internal_mail_boxes: HashMap<u64, LooseBoundedSender<Msg>>,
    range_state: Arc<Mutex<RangeState>>,
    external_routers: PlaceHolder,
    pending_votes: Vec<RaftMessage>,
    pending_cross_snap: HashMap<u64, metapb::RegionEpoch>,
    poller: CpuPool,
}

impl StoreState {
    fn handle_stale_msg(
        trans: &PlaceHolder,
        msg: &RaftMessage,
        cur_epoch: &RegionEpoch,
        need_gc: bool,
        target_region: Option<Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        if !need_gc {
            info!(
                "[region {}] raft message {:?} is stale, current {:?}, ignore it",
                region_id, msg_type, cur_epoch
            );
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["stale_msg"])
                .inc_by(1);
            return;
        }

        info!(
            "[region {}] raft message {:?} is stale, current {:?}, tell to gc",
            region_id, msg_type, cur_epoch
        );

        let mut gc_msg = RaftMessage::new();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch.clone());
        if let Some(r) = target_region {
            gc_msg.set_merge_target(r);
        } else {
            gc_msg.set_is_tombstone(true);
        }
        if let Err(e) = trans.send(gc_msg) {
            error!("[region {}] send gc message failed {:?}", region_id, e);
        }
    }

    /// This method should only be called when
    fn sender(&mut self, region_id: u64, msg: &Msg) -> Option<LooseBoundedSender<Msg>> {
        let slot = match self.internal_mail_boxes.entry(region_id) {
            Entry::Occupied(o) => return Some(o.get().clone()),
            Entry::Vacant(v) => v,
        };
        let msg = match msg {
            Msg::RaftMessage(ref msg) => msg,
            _ => return None,
        };
        let to_peer = msg.get_to_peer();
        let message = msg.get_message();
        let msg_type = message.get_msg_type();
        let is_vote_msg = msg_type == MessageType::MsgRequestVote;
        let from_store_id = msg.get_from_peer().get_store_id();
        let from_epoch = msg.get_region_epoch();
        let state_key = keys::region_state_key(region_id);
        let state: Option<RegionLocalState> =
            match self.meta.engines.kv.get_msg_cf(CF_RAFT, &state_key) {
                Ok(s) => s,
                Err(e) => {
                    error!("[region {}] failed to load local state: {:?}", region_id, e);
                    return None;
                }
            };
        if let Some(local_state) = state {
            if local_state.get_state() != PeerState::Tombstone {
                // Maybe split, but not registered yet.
                STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                    .with_label_values(&["region_nonexistent"])
                    .inc_by(1);
                if util::is_first_vote_msg(msg) {
                    self.pending_votes.push(msg.to_owned());
                    info!(
                        "[region {}] doesn't exist yet, wait for it to be split",
                        region_id
                    );
                    return None;
                }
                error!(
                    "[region {}] region not exist but not tombstone: {:?}",
                    region_id, local_state
                );
                return None;
            }
            debug!("[region {}] tombstone state: {:?}", region_id, local_state);
            let region = local_state.get_region();
            let region_epoch = region.get_region_epoch();
            if local_state.has_merge_state() {
                info!(
                    "[region {}] merged peer [epoch: {:?}] receive a stale message {:?}",
                    region_id, region_epoch, msg_type
                );

                let merge_target = if let Some(peer) = util::find_peer(region, from_store_id) {
                    assert_eq!(peer, msg.get_from_peer());
                    // Let stale peer decides whether it should wait for merging or just remove
                    // itself.
                    Some(local_state.get_merge_state().get_target().to_owned())
                } else {
                    // If a peer is isolated before prepare_merge and conf remove, it should just
                    // remove itself.
                    None
                };
                Self::handle_stale_msg(
                    &self.external_routers,
                    msg,
                    region_epoch,
                    true,
                    merge_target,
                );
                return None;
            }
            // The region in this peer is already destroyed
            if util::is_epoch_stale(from_epoch, region_epoch) {
                info!(
                    "[region {}] tombstone peer [epoch: {:?}] \
                     receive a stale message {:?}",
                    region_id, region_epoch, msg_type,
                );

                let not_exist = util::find_peer(region, from_store_id).is_none();
                Self::handle_stale_msg(
                    &self.external_routers,
                    msg,
                    region_epoch,
                    is_vote_msg && not_exist,
                    None,
                );

                return None;
            }

            if from_epoch.get_conf_ver() == region_epoch.get_conf_ver() {
                STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                    .with_label_values(&["region_tombstone_peer"])
                    .inc_by(1);
                error!(
                    "tombstone peer [epoch: {:?}] receive an invalid \
                     message {:?}, ignore it",
                    region_epoch, msg_type
                );
                return None;
            }
        }
        if msg_type != MessageType::MsgRequestVote
            && (msg_type != MessageType::MsgHeartbeat || message.get_commit() != INVALID_INDEX)
        {
            debug!(
                "target peer {:?} doesn't exist, stale message {:?}.",
                to_peer, msg_type
            );
            STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                .with_label_values(&["stale_msg"])
                .inc_by(1);
            return None;
        }

        let start_key = data_key(msg.get_start_key());
        {
            let range_state = self.range_state.lock().unwrap();
            if let Some((_, &exist_region_id)) = range_state
                .region_ranges
                .range((Excluded(start_key), Unbounded::<Vec<u8>>))
                .next()
            {
                let exist_region = &range_state.region_peers[&exist_region_id];
                if enc_start_key(exist_region) < data_end_key(msg.get_end_key()) {
                    debug!("msg {:?} is overlapped with region {:?}", msg, exist_region);
                    if util::is_first_vote_msg(msg) {
                        self.pending_votes.push(msg.to_owned());
                    }
                    STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC
                        .with_label_values(&["region_overlap"])
                        .inc_by(1);
                    self.pending_cross_snap
                        .insert(region_id, msg.get_region_epoch().to_owned());
                    return None;
                }
            }
        }

        // New created peers should know it's learner or not.
        let peer = match Peer::replicate(&self.meta, region_id, to_peer.clone()) {
            Ok(peer) => peer,
            Err(e) => {
                error!("[region {}] failed to create peer: {:?}", region_id, e);
                return None;
            }
        };
        let (sender, holder) = PeerHolder::new(peer);
        self.poller.spawn(holder).forget();
        Some(slot.insert(sender).clone())
    }
}

pub struct LocalTransport {
    states: Arc<Mutex<StoreState>>,
    cache: HashMap<u64, LooseBoundedSender<Msg>>,
}

impl LocalTransport {
    #[inline]
    fn get_sender(&mut self, region_id: u64, msg: &Msg) -> Option<&mut LooseBoundedSender<Msg>> {
        match self.cache.entry(region_id) {
            Entry::Occupied(o) => Some(o.into_mut()),
            Entry::Vacant(v) => {
                let sender = {
                    let mut states = self.states.lock().unwrap();
                    match states.sender(region_id, &msg) {
                        None => return None,
                        Some(s) => s,
                    }
                };
                Some(v.insert(sender))
            }
        }
    }

    pub fn send(&mut self, region_id: u64, msg: Msg) -> Result<(), TrySendError<Msg>> {
        if let Some(sender) = self.get_sender(region_id, &msg) {
            sender.try_send(msg)
        } else {
            unimplemented!()
        }
    }

    pub fn force_send(&mut self, region_id: u64, msg: Msg) -> Result<(), SendError<Msg>> {
        if let Some(sender) = self.get_sender(region_id, &msg) {
            sender.force_send(msg)
        } else {
            unimplemented!()
        }
    }
}
