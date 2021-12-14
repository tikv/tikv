// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::worker::split_check::{finish_split, split_shard_files};
use crate::store::{Callback, MsgWaitFollowerSplitFiles, PeerMsg, RegionTag};
use crate::RaftRouter;
use bytes::Bytes;
use kvenginepb::SplitStage;
use kvproto::metapb;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use tikv_util::mpsc::Receiver;
use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{error, info};

/// Region related task
#[derive(Debug)]
pub enum Task {
    ApplyChangeSet {
        change: kvenginepb::ChangeSet,
    },
    RecoverSplit {
        region: metapb::Region,
        peer: metapb::Peer,
        split_keys: Vec<Bytes>,
        stage: kvenginepb::SplitStage,
    },
    FinishSplit {
        region: metapb::Region,
        wait: MsgWaitFollowerSplitFiles,
    },
    RejectChangeSet {
        change: kvenginepb::ChangeSet,
    },
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
            Task::FinishSplit { region, wait } => write!(
                f,
                "FinishSplit {}:{}",
                region.get_id(),
                region.get_region_epoch().get_version(),
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
    apply_scheduler: Scheduler<ApplyTask>,
}

impl Runner {
    pub fn new(
        kv: kvengine::Engine,
        router: RaftRouter,
        apply_scheduler: Scheduler<ApplyTask>,
    ) -> Self {
        Self {
            kv,
            router,
            rejects: HashMap::new(),
            apply_scheduler,
        }
    }

    fn prepare_region_resource(&self, task: &Task) -> Option<Receiver<crate::Result<Task>>> {
        todo!()
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        let desc = task.to_string();
        match &task {
            Task::ApplyChangeSet { change } => {
                if let Some(receiver) = self.prepare_region_resource(&task) {
                    self.apply_scheduler.schedule(ApplyTask { desc, receiver });
                }
            }
            _ => {
                let (sender, receiver) = tikv_util::mpsc::bounded(1);
                sender.send(Ok(task));
                self.apply_scheduler.schedule(ApplyTask { desc, receiver });
            }
        }
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

    pub fn handle_apply_change_set(&self, change: &kvenginepb::ChangeSet) -> crate::Result<()> {
        if change.has_snapshot() {
            let ingest_tree = kvengine::IngestTree {
                change_set: change.clone(),
                passive: true,
            };
            self.kv.ingest(ingest_tree)?;
            return Ok(());
        }
        self.kv.apply_change_set(change.clone())?;
        Ok(())
    }

    pub fn handle_recover_split(
        &self,
        region: metapb::Region,
        peer: metapb::Peer,
        split_keys: Vec<Bytes>,
        stage: kvenginepb::SplitStage,
    ) -> crate::Result<()> {
        let tag = RegionTag::from_region(&region);
        info!("handle recover split"; "region" => tag);
        match stage {
            SplitStage::Initial => {}
            SplitStage::PreSplit => {
                panic!("region {} the region worker will be blocked", tag);
            }
            SplitStage::PreSplitFlushDone => {
                split_shard_files(&self.router, &self.kv, &region, &peer)?;
            }
            SplitStage::SplitFileDone => {}
        }
        let msg = MsgWaitFollowerSplitFiles {
            split_keys,
            callback: Callback::None,
        };
        self.router
            .send(region.get_id(), PeerMsg::WaitFollowerSplitFiles(msg));
        Ok(())
    }
}

pub struct ApplyTask {
    desc: String,
    receiver: Receiver<crate::Result<Task>>,
}

impl Display for ApplyTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&self.desc)
    }
}

impl Runnable for ApplyRunner {
    type Task = ApplyTask;
    fn run(&mut self, task: ApplyTask) {
        let res = task.receiver.recv().unwrap();
        if res.is_err() {
            let err = res.err().unwrap();
            error!("failed to prepare snapshot resource"; "err" => ?err);
            return;
        }
        let task = res.unwrap();
        let res = match task {
            Task::ApplyChangeSet { change } => self.handle_apply_change_set(&change),
            Task::RecoverSplit {
                region,
                peer,
                split_keys,
                stage,
            } => self.handle_recover_split(region, peer, split_keys, stage),
            Task::FinishSplit { region, wait } => {
                finish_split(&self.router, &region, wait.split_keys)
            }
            Task::RejectChangeSet { .. } => unreachable!(),
        };
    }
}
