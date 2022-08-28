// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, mem, sync::Arc};

use engine_traits::{KvEngine, OpenOptions, RaftEngine, TabletFactory};
use kvproto::{metapb, raft_serverpb::RegionLocalState};
use raft::{RawNode, StateRole, INVALID_ID};
use raftstore::store::{util::find_peer, Config, EntryStorage, RaftlogFetchTask, WriteRouter};
use slog::{o, Logger};
use tikv_util::{box_err, config::ReadableSize, worker::Scheduler};

use super::storage::Storage;
use crate::{
    operation::AsyncWriter,
    tablet::{self, CachedTablet},
    Result,
};

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    raft_group: RawNode<Storage<ER>>,
    tablet: CachedTablet<EK>,
    /// We use a cache for looking up peers. Not all peers exist in region's
    /// peer list, for example, an isolated peer may need to send/receive
    /// messages with unknown peers after recovery.
    peer_cache: Vec<metapb::Peer>,
    pub(crate) async_writer: AsyncWriter<EK, ER>,
    has_ready: bool,
    pub(crate) logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Creates a new peer.
    ///
    /// If peer is destroyed, `None` is returned.
    pub fn new(
        cfg: &Config,
        region_id: u64,
        store_id: u64,
        tablet_factory: &dyn TabletFactory<EK>,
        engine: ER,
        scheduler: Scheduler<RaftlogFetchTask>,
        logger: &Logger,
    ) -> Result<Option<Self>> {
        let s = match Storage::new(region_id, store_id, engine, scheduler, logger)? {
            Some(s) => s,
            None => return Ok(None),
        };
        let logger = s.logger().clone();

        let applied_index = s.apply_state().get_applied_index();
        let peer_id = s.peer().get_id();

        let raft_cfg = raft::Config {
            id: peer_id,
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            min_election_tick: cfg.raft_min_election_timeout_ticks,
            max_election_tick: cfg.raft_max_election_timeout_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg.0,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            skip_bcast_commit: true,
            pre_vote: cfg.prevote,
            max_committed_size_per_ready: ReadableSize::mb(16).0,
            ..Default::default()
        };

        let tablet_index = s.region_state().get_tablet_index();
        // Another option is always create tablet even if tablet index is 0. But this
        // can introduce race when gc old tablet and create new peer.
        let tablet = if tablet_index != 0 {
            if !tablet_factory.exists(region_id, tablet_index) {
                return Err(box_err!(
                    "missing tablet {} for region {}",
                    tablet_index,
                    region_id
                ));
            }
            // TODO: Perhaps we should stop create the tablet automatically.
            Some(tablet_factory.open_tablet(
                region_id,
                Some(tablet_index),
                OpenOptions::default().set_create(true),
            )?)
        } else {
            None
        };

        let mut peer = Peer {
            raft_group: RawNode::new(&raft_cfg, s, &logger)?,
            tablet: CachedTablet::new(tablet),
            has_ready: false,
            async_writer: AsyncWriter::new(region_id, peer_id),
            logger,
            peer_cache: vec![],
        };

        // If this region has only one peer and I am the one, campaign directly.
        let region = peer.region();
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        Ok(Some(peer))
    }

    #[inline]
    pub fn region(&self) -> &metapb::Region {
        self.raft_group.store().region()
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.region().get_id()
    }

    #[inline]
    pub fn peer(&self) -> &metapb::Peer {
        self.raft_group.store().peer()
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer().get_id()
    }

    #[inline]
    pub fn storage(&self) -> &Storage<ER> {
        self.raft_group.store()
    }

    #[inline]
    pub fn storage_mut(&mut self) -> &mut Storage<ER> {
        self.raft_group.mut_store()
    }

    #[inline]
    pub fn entry_storage(&self) -> &EntryStorage<ER> {
        self.raft_group.store().entry_storage()
    }

    #[inline]
    pub fn entry_storage_mut(&mut self) -> &mut EntryStorage<ER> {
        self.raft_group.mut_store().entry_storage_mut()
    }

    #[inline]
    pub fn tablet(&self) -> &CachedTablet<EK> {
        &self.tablet
    }

    #[inline]
    pub fn tablet_mut(&mut self) -> &mut CachedTablet<EK> {
        &mut self.tablet
    }

    #[inline]
    pub fn raft_group(&self) -> &RawNode<Storage<ER>> {
        &self.raft_group
    }

    #[inline]
    pub fn raft_group_mut(&mut self) -> &mut RawNode<Storage<ER>> {
        &mut self.raft_group
    }

    /// Mark the peer has a ready so it will be checked at the end of every
    /// processing round.
    #[inline]
    pub fn set_has_ready(&mut self) {
        self.has_ready = true;
    }

    /// Mark the peer has no ready and return its previous state.
    #[inline]
    pub fn reset_has_ready(&mut self) -> bool {
        mem::take(&mut self.has_ready)
    }

    #[inline]
    pub fn insert_peer_cache(&mut self, peer: metapb::Peer) {
        for p in self.raft_group.store().region().get_peers() {
            if p.get_id() == peer.get_id() {
                return;
            }
        }
        for p in &mut self.peer_cache {
            if p.get_id() == peer.get_id() {
                *p = peer;
                return;
            }
        }
        self.peer_cache.push(peer);
    }

    #[inline]
    pub fn clear_peer_cache(&mut self) {
        self.peer_cache.clear();
    }

    #[inline]
    pub fn peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        for p in self.raft_group.store().region().get_peers() {
            if p.get_id() == peer_id {
                return Some(p.clone());
            }
        }
        self.peer_cache
            .iter()
            .find(|p| p.get_id() == peer_id)
            .cloned()
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    /// Get the leader peer meta.
    ///
    /// `None` is returned if there is no leader or the meta can't be found.
    #[inline]
    pub fn leader(&self) -> Option<metapb::Peer> {
        let leader_id = self.leader_id();
        if leader_id != 0 {
            self.peer_from_cache(leader_id)
        } else {
            None
        }
    }

    /// Term of the state machine.
    #[inline]
    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }
}
