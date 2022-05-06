// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use engine_traits::KvEngine;
use pd_client::PdClient;
use tikv_util::worker::Runnable;

use super::{
    cleanup_sst::{Runner as CleanupSstRunner, Task as CleanupSstTask},
    compact::{Runner as CompactRunner, Task as CompactTask},
};
use crate::store::StoreRouter;

pub enum Task {
    Compact(CompactTask),
    CleanupSst(CleanupSstTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Compact(ref t) => t.fmt(f),
            Task::CleanupSst(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<E, C, S>
where
    E: KvEngine,
    S: StoreRouter<E>,
{
    compact: CompactRunner<E>,
    cleanup_sst: CleanupSstRunner<E, C, S>,
}

impl<E, C, S> Runner<E, C, S>
where
    E: KvEngine,
    C: PdClient,
    S: StoreRouter<E>,
{
    pub fn new(
        compact: CompactRunner<E>,
        cleanup_sst: CleanupSstRunner<E, C, S>,
    ) -> Runner<E, C, S> {
        Runner {
            compact,
            cleanup_sst,
        }
    }
}

impl<E, C, S> Runnable for Runner<E, C, S>
where
    E: KvEngine,
    C: PdClient,
    S: StoreRouter<E>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
            Task::CleanupSst(t) => self.cleanup_sst.run(t),
        }
    }
}
