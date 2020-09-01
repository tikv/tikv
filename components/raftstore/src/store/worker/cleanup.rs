// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use super::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};
use super::compact::{Runner as CompactRunner, Task as CompactTask};

use crate::store::StoreRouter;
use engine_traits::KvEngine;
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

pub struct Runner<KE, PC, SR> {
    compact: CompactRunner<KE>,
    cleanup_sst: CleanupSSTRunner<PC, SR>,
}

impl<KE, PC, SR> Runner<KE, PC, SR>
where
    PC: PdClient,
    SR: StoreRouter,
{
    pub fn new(
        compact: CompactRunner<KE>,
        cleanup_sst: CleanupSSTRunner<PC, SR>,
    ) -> Runner<KE, PC, SR> {
        Runner {
            compact,
            cleanup_sst,
        }
    }
}

impl<KE, PC, SR> Runnable for Runner<KE, PC, SR>
where
    KE: KvEngine,
    PC: PdClient,
    SR: StoreRouter,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
            Task::CleanupSST(t) => self.cleanup_sst.run(t),
        }
    }
}
