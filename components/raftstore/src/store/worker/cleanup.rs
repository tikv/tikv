// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use super::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};

use crate::store::StoreRouter;
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

pub struct Runner<C, S> {
    cleanup_sst: CleanupSSTRunner<C, S>,
}

impl<C, S> Runner<C, S>
where
    C: PdClient,
    S: StoreRouter,
{
    pub fn new(cleanup_sst: CleanupSSTRunner<C, S>) -> Runner<C, S> {
        Runner { cleanup_sst }
    }
}

impl<C, S> Runnable<Task> for Runner<C, S>
where
    C: PdClient,
    S: StoreRouter,
{
    fn run(&mut self, task: Task) {
        match task {
            Task::CleanupSST(t) => self.cleanup_sst.run(t),
        }
    }
}
