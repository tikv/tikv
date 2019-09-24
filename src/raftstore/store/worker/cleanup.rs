// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use super::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};
use super::compact::{Runner as CompactRunner, Task as CompactTask};

use crate::raftstore::store::StoreRouter;
use pd_client::PdClient;
use tikv_util::worker::Runnable;

pub enum Task {
    Compact(CompactTask),
    CleanupSST(CleanupSSTTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Compact(ref t) => t.fmt(f),
            Task::CleanupSST(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<C, S> {
    compact: CompactRunner,
    cleanup_sst: CleanupSSTRunner<C, S>,
}

impl<C: PdClient, S: StoreRouter> Runner<C, S> {
    pub fn new(compact: CompactRunner, cleanup_sst: CleanupSSTRunner<C, S>) -> Runner<C, S> {
        Runner {
            compact,
            cleanup_sst,
        }
    }
}

impl<C: PdClient, S: StoreRouter> Runnable<Task> for Runner<C, S> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
            Task::CleanupSST(t) => self.cleanup_sst.run(t),
        }
    }
}
