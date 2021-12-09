// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use online_config::ConfigChange;
use std::fmt::{self, Display, Formatter};

use crate::RaftRouter;
use raftstore::coprocessor::Config;
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer};

#[derive(Debug)]
pub struct SplitCheckTask {
    region_id: u64,
    max_size: u64,
}

impl Display for SplitCheckTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[split check worker] Split Check Task for {}, max_size: {}",
            self.region_id, self.max_size,
        )
    }
}

impl SplitCheckTask {
    pub fn new(region_id: u64, max_size: u64) -> SplitCheckTask {
        Self {
            region_id,
            max_size,
        }
    }
}

pub struct SplitCheckRunner {
    kv: kvengine::Engine,
    router: RaftRouter,
}

impl SplitCheckRunner {
    pub fn new(kv: kvengine::Engine, router: RaftRouter) -> Self {
        Self { kv, router }
    }
}

impl Runnable for SplitCheckRunner {
    type Task = SplitCheckTask;

    fn run(&mut self, task: SplitCheckTask) {
        todo!()
    }
}
