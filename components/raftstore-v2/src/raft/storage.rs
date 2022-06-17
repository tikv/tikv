// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::RaftEngine;
use raft::{
    eraftpb::{Entry, Snapshot},
    GetEntriesContext, RaftState,
};
use slog::Logger;

/// A storage for raft.
pub struct Storage<ER> {
    engine: ER,
    logger: Logger,
}

impl<ER> Storage<ER> {
    pub fn new(engine: ER, logger: Logger) -> Storage<ER> {
        Storage { engine, logger }
    }

    pub fn applied_index(&self) -> u64 {
        unimplemented!()
    }
}

impl<ER: RaftEngine> raft::Storage for Storage<ER> {
    fn initial_state(&self) -> raft::Result<RaftState> {
        unimplemented!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        unimplemented!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        unimplemented!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        unimplemented!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        unimplemented!()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        unimplemented!()
    }
}
