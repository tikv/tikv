// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use engine_traits::{KvEngine, RaftEngine};
use pd_client::PdClient;
use tikv_util::worker::Runnable;

use super::{
    cleanup_snapshot::{Runner as GcSnapshotRunner, Task as GcSnapshotTask},
    cleanup_sst::{Runner as CleanupSstRunner, Task as CleanupSstTask},
    compact::{Runner as CompactRunner, Task as CompactTask},
};
use crate::store::StoreRouter;

pub enum Task {
    Compact(CompactTask),
    CleanupSst(CleanupSstTask),
    GcSnapshot(GcSnapshotTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Compact(ref t) => t.fmt(f),
            Task::CleanupSst(ref t) => t.fmt(f),
            Task::GcSnapshot(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<E, R, S>
where
    E: KvEngine,
    R: RaftEngine,
    S: StoreRouter<E>,
{
    compact: CompactRunner<E>,
    cleanup_sst: CleanupSstRunner,
    gc_snapshot: GcSnapshotRunner<E, R>,
}

impl<E, R, S> Runner<E, R, S>
where
    E: KvEngine,
    R: RaftEngine,
    S: StoreRouter<E>,
{
    pub fn new(
        compact: CompactRunner<E>,
        cleanup_sst: CleanupSstRunner,
        gc_snapshot: GcSnapshotRunner<E, R>,
    ) -> Runner<E, R, S> {
        Runner {
            compact,
            cleanup_sst,
            gc_snapshot,
        }
    }
}

impl<E, R, S> Runnable for Runner<E, R, S>
where
    E: KvEngine,
    R: RaftEngine,
    S: StoreRouter<E>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
            Task::CleanupSst(t) => self.cleanup_sst.run(t),
            Task::GcSnapshot(t) => self.gc_snapshot.run(t),
        }
    }
}
