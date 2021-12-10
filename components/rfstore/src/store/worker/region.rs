// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::RaftRouter;
use bytes::Bytes;
use kvproto::metapb;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display, Formatter};
use tikv_util::mpsc::{Receiver, Sender};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

/// Region related task
#[derive(Debug)]
pub enum Task {
    ApplyChangeSet {
        change: kvenginepb::ChangeSet,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
    RecoverSplit {
        region: metapb::Region,
        peer: metapb::Peer,
        split_keys: Vec<Bytes>,
        stage: kvenginepb::SplitStage,
    },
    RejectChangeSet {
        change: kvenginepb::ChangeSet,
    },
}

impl Task {
    pub fn destroy(region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Task {
        Task::Destroy {
            region_id,
            start_key,
            end_key,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Task::ApplyChangeSet { change } => write!(
                f,
                "Snap apply for {}:{}",
                change.get_shard_id(),
                change.get_shard_ver()
            ),
            Task::Destroy {
                region_id,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Destroy {} [{}, {})",
                region_id,
                log_wrappers::Value::key(&start_key),
                log_wrappers::Value::key(&end_key)
            ),
            Task::RecoverSplit { .. } => {
                todo!()
            }
            Task::RejectChangeSet { .. } => {
                todo!()
            }
        }
    }
}

struct ChangeSetKey {
    region_id: u64,
    version: u64,
}

pub struct Runner {
    kv: kvengine::Engine,
    router: RaftRouter,
    rejects: HashMap<ChangeSetKey, kvenginepb::ChangeSet>,
    apply_scheduler: Scheduler<Task>,
}

impl Runner {
    pub fn new(
        kv: kvengine::Engine,
        router: RaftRouter,
        apply_scheduler: Scheduler<Task>,
    ) -> (Self) {
        Self {
            kv,
            router,
            rejects: HashMap::new(),
            apply_scheduler,
        }
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        todo!()
    }
}

impl RunnableWithTimer for Runner {
    fn on_timeout(&mut self) {
        todo!()
    }

    fn get_interval(&self) -> Duration {
        todo!()
    }
}

pub struct ApplyRunner {
    kv: kvengine::Engine,
    router: RaftRouter,
}

impl ApplyRunner {
    pub fn new(kv: kvengine::Engine, router: RaftRouter) -> Self {
        Self { kv, router }
    }
}

impl Runnable for ApplyRunner {
    type Task = Task;
    fn run(&mut self, task: Task) {
        todo!()
    }
}

impl RunnableWithTimer for ApplyRunner {
    fn on_timeout(&mut self) {
        todo!()
    }

    fn get_interval(&self) -> Duration {
        todo!()
    }
}
