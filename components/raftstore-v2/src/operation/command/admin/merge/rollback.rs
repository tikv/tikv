// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use slog::warn;

use crate::raft::Peer;

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_reject_commit_merge(&mut self, index: u64) {
        warn!(self.logger, "target peer rejected commit merge"; "index" => index);
    }
}
