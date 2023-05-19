// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

pub use compact::{CompactThreshold, Runner as CompactRunner, Task as CompactTask};
use engine_traits::KvEngine;
use tikv_util::worker::Runnable;

mod compact;

pub enum Task {
    Compact(CompactTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Compact(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<E: KvEngine> {
    compact: CompactRunner<E>,
    // todo: more cleanup related runner may be added later
}

impl<E: KvEngine> Runner<E> {
    pub fn new(compact: CompactRunner<E>) -> Runner<E> {
        Runner { compact }
    }
}

impl<E: KvEngine> Runnable for Runner<E> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
        }
    }
}
