// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::RaftEngine;
use kvproto::metapb;
use raft::RawNode;
use raftstore::store::Config;
use slog::{o, Logger};
use tikv_util::{box_err, config::ReadableSize};

use super::storage::Storage;
use crate::Result;

/// A peer that delegates commands between state machine and raft.
pub struct Peer<ER: RaftEngine> {
    region_id: u64,
    peer: metapb::Peer,
    raft_group: RawNode<Storage<ER>>,
    logger: Logger,
}

impl<ER: RaftEngine> Peer<ER> {
    pub fn new(
        cfg: &Config,
        store_id: u64,
        region: metapb::Region,
        engine: ER,
        logger: Logger,
    ) -> Result<Self> {
        let peer = region
            .get_peers()
            .iter()
            .find(|p| p.get_store_id() == store_id && p.get_id() != raft::INVALID_ID);
        let peer = match peer {
            Some(p) => p,
            None => return Err(box_err!("no valid peer found in {:?}", region.get_peers())),
        };
        let l = logger.new(o!("peer_id" => peer.id));

        let ps = Storage::new(engine, l.clone());

        let applied_index = ps.applied_index();

        let raft_cfg = raft::Config {
            id: peer.get_id(),
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

        Ok(Peer {
            region_id: region.get_id(),
            peer: peer.clone(),
            raft_group: RawNode::new(&raft_cfg, ps, &logger)?,
            logger: l,
        })
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }
}
