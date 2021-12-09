// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use super::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};

use crate::store::StoreRouter;
use engine_traits::KvEngine;
use pd_client::PdClient;
use tikv_util::worker::Runnable;

pub enum Task {
    CleanupSST(CleanupSSTTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::CleanupSST(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<E, C, S>
where
    E: KvEngine,
    S: StoreRouter<E>,
{
    cleanup_sst: CleanupSSTRunner<E, C, S>,
}

impl<E, C, S> Runner<E, C, S>
where
    E: KvEngine,
    C: PdClient,
    S: StoreRouter<E>,
{
    pub fn new(cleanup_sst: CleanupSSTRunner<E, C, S>) -> Runner<E, C, S> {
        Runner { cleanup_sst }
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
            Task::CleanupSST(t) => self.cleanup_sst.run(t),
        }
    }
}
