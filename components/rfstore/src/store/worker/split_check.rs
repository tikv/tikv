// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use kvproto::metapb;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use online_config::ConfigChange;
use std::fmt::{self, Display, Formatter};

use crate::store::{Callback, RegionTag};
use crate::RaftRouter;
use raftstore::coprocessor::Config;
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tikv_util::{error, info, warn};

#[derive(Debug)]
pub struct SplitCheckTask {
    region: metapb::Region,
    peer: metapb::Peer,
    max_size: u64,
}

impl Display for SplitCheckTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[split check worker] Split Check Task for {}, max_size: {}",
            RegionTag::from_region(&self.region),
            self.max_size,
        )
    }
}

impl SplitCheckTask {
    pub fn new(region: metapb::Region, peer: metapb::Peer, max_size: u64) -> SplitCheckTask {
        Self {
            region,
            peer,
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
        let region = task.region;
        let shard = self.kv.get_shard(region.get_id());
        let tag = RegionTag::from_region(&region);
        if shard.is_none() {
            warn!("split check shard not found"; "region" => tag);
            return;
        }
        let shard = shard.unwrap();
        let keys = shard.get_suggest_split_keys(task.max_size);
        if keys.len() == 0 {
            warn!("split check got empty split keys"; "region" => tag);
            return;
        }
        info!(
            "split region by checker";
            "region" => tag,
        );
        let _ = split_engine_and_region(
            &self.router,
            &self.kv,
            &region,
            &task.peer,
            keys,
            Callback::None,
        );
    }
}

// splitEngineAndRegion execute the complete procedure to split a region.
// 1. execute PreSplit on raft command.
// 2. Split the engine files.
// 3. Split the region.
pub fn split_engine_and_region(
    router: &RaftRouter,
    kv: &kvengine::Engine,
    region: &metapb::Region,
    peer: &metapb::Peer,
    keys: Vec<Bytes>,
    callback: Callback,
) -> crate::Result<()> {
    todo!()
}
