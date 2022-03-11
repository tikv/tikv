// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::{MsgApplyChangeSetResult, PeerMsg, RegionIDVer};
use crate::RaftRouter;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use tikv_util::mpsc::Receiver;
use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{error, info, warn};

/// Region related task
#[derive(Debug)]
pub enum Task {
    ApplyChangeSet { change: kvenginepb::ChangeSet },
    RejectChangeSet { change: kvenginepb::ChangeSet },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Task::ApplyChangeSet { change } => write!(
                f,
                "ApplyChangeSet for {}:{}",
                change.get_shard_id(),
                change.get_shard_ver()
            ),
            Task::RejectChangeSet { change, .. } => write!(
                f,
                "reject change set for {}",
                RegionIDVer::new(change.get_shard_id(), change.get_shard_ver()),
            ),
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct ChangeSetKey {
    region_id: u64,
    sequence: u64,
}

impl ChangeSetKey {
    fn new(region_id: u64, sequence: u64) -> Self {
        Self {
            region_id,
            sequence,
        }
    }
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

    fn prepare_region_resource(
        &mut self,
        mut change_set: kvenginepb::ChangeSet,
    ) -> Option<Receiver<crate::Result<Task>>> {
        let kv = &self.kv;
        let change_set_key =
            ChangeSetKey::new(change_set.get_shard_id(), change_set.get_sequence());
        let id_ver = RegionIDVer::new(change_set.get_shard_id(), change_set.get_shard_ver());
        if let Some(meta_change) = self.rejects.remove(&change_set_key) {
            if let Some(shard) = self.kv.get_shard(change_set.get_shard_id()) {
                warn!("shard reject change set";
                    "region" => id_ver,
                    "change_set" => ?&change_set,
                    "initial_flushed" => shard.get_initial_flushed(),
                );
                if !meta_change.has_compaction() || !meta_change.get_compaction().get_conflicted() {
                    return None;
                }
                change_set.mut_compaction().set_conflicted(true);
                let (tx, rx) = tikv_util::mpsc::bounded(1);
                tx.send(Ok(Task::ApplyChangeSet { change: change_set }))
                    .unwrap();
                return Some(rx);
            }
            warn!("shard not found for prepare change set resource";
                "change_set" => ?&change_set,
                "region" => id_ver);
            return None;
        }
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let res = kv
            .pre_load_files(&change_set)
            .map_err(|err| crate::Error::KVEngineError(err));
        tx.send(res.map(|_| Task::ApplyChangeSet { change: change_set }))
            .unwrap();
        Some(rx)
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        let desc = task.to_string();
        match task {
            Task::ApplyChangeSet { change } => {
                let id_ver = RegionIDVer::new(change.shard_id, change.shard_ver);
                if let Some(receiver) = self.prepare_region_resource(change) {
                    self.apply_scheduler
                        .schedule(ApplyTask {
                            desc,
                            receiver,
                            id_ver,
                        })
                        .unwrap();
                }
            }
            Task::RejectChangeSet { change } => {
                let key = ChangeSetKey::new(change.get_shard_id(), change.get_sequence());
                self.rejects.insert(key, change);
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
                active: false,
            };
            self.kv.ingest(ingest_tree)?;
            return Ok(());
        }
        self.kv.apply_change_set(change.clone())?;
        Ok(())
    }
}

pub struct ApplyTask {
    desc: String,
    receiver: Receiver<crate::Result<Task>>,
    id_ver: RegionIDVer,
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
        let msg;
        if res.is_err() {
            let err = res.err().unwrap();
            let err_msg = format!("{:?}", &err);
            error!("failed to prepare change set resource"; "err" => ?err);
            msg = PeerMsg::ApplyChangeSetResult(MsgApplyChangeSetResult {
                result: Err(err_msg),
            });
        } else {
            let task = res.unwrap();
            match task {
                Task::ApplyChangeSet { change } => {
                    let result = self
                        .handle_apply_change_set(&change)
                        .map(|_| change)
                        .map_err(|e| format!("{:?}", e));
                    msg = PeerMsg::ApplyChangeSetResult(MsgApplyChangeSetResult { result });
                }
                Task::RejectChangeSet { .. } => unreachable!(),
            };
        }
        if let Err(err) = self.router.send(task.id_ver.id(), msg) {
            warn!(
                "failed to send apply change set result for {}, error {:?}",
                task.id_ver, err
            );
        }
    }
}
