// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::RaftEngine;
use kvproto::metapb;
use raftstore::store::Config;
use slog::Logger;

use crate::{raft::Peer, Result};

pub struct PeerFsm<ER: RaftEngine> {
    peer: Peer<ER>,
}

impl<ER: RaftEngine> PeerFsm<ER> {
    pub fn new(peer: Peer<ER>) -> Result<Self> {
        Ok(PeerFsm { peer })
    }

    pub fn logger(&self) -> &Logger {
        self.peer.logger()
    }
}
