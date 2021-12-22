// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use super::cleanup_snapshot::{Runner as GcSnapshotRunner, Task as GcSnapshotTask};
use super::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};
use super::compact::{Runner as CompactRunner, Task as CompactTask};

use crate::store::StoreRouter;
use engine_traits::{KvEngine, RaftEngine};
use pd_client::PdClient;
use tikv_util::worker::Runnable;

pub enum Task {
    Compact(CompactTask),
    CleanupSST(CleanupSSTTask),
    GcSnapshot(GcSnapshotTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Compact(ref t) => t.fmt(f),
            Task::CleanupSST(ref t) => t.fmt(f),
            Task::GcSnapshot(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<E, R, C, S>
where
    E: KvEngine,
    R: RaftEngine,
    S: StoreRouter<E>,
{
    compact: CompactRunner<E>,
    cleanup_sst: CleanupSSTRunner<E, C, S>,
    gc_snapshot: GcSnapshotRunner<E, R>,
}

impl<E, R, C, S> Runner<E, R, C, S>
where
    E: KvEngine,
    R: RaftEngine,
    C: PdClient,
    S: StoreRouter<E>,
{
    pub fn new(
        compact: CompactRunner<E>,
        cleanup_sst: CleanupSSTRunner<E, C, S>,
        gc_snapshot: GcSnapshotRunner<E, R>,
    ) -> Runner<E, R, C, S> {
        Runner {
            compact,
            cleanup_sst,
            gc_snapshot,
        }
    }
}

impl<E, R, C, S> Runnable for Runner<E, R, C, S>
where
    E: KvEngine,
    R: RaftEngine,
    C: PdClient,
    S: StoreRouter<E>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
            Task::CleanupSST(t) => self.cleanup_sst.run(t),
            Task::GcSnapshot(t) => self.gc_snapshot.run(t),
        }
    }
}
