// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use super::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};
use super::compact::{Runner as CompactRunner, Task as CompactTask};

use engine_traits::KvEngine;
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

pub struct Runner<E>
where
    E: KvEngine,
{
    compact: CompactRunner<E>,
    cleanup_sst: CleanupSSTRunner,
}

impl<E> Runner<E>
where
    E: KvEngine,
{
    pub fn new(compact: CompactRunner<E>, cleanup_sst: CleanupSSTRunner) -> Runner<E> {
        Runner {
            compact,
            cleanup_sst,
        }
    }
}

impl<E> Runnable for Runner<E>
where
    E: KvEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
            Task::CleanupSST(t) => self.cleanup_sst.run(t),
        }
    }
}
