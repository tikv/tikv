// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::cmd_resp::err_resp;
use crate::store::worker::split_check::{finish_split, split_shard_files};
use crate::store::{Callback, MsgWaitFollowerSplitFiles, PeerMsg, RegionIDVer};
use crate::RaftRouter;
use bytes::Bytes;
use kvenginepb::SplitStage;
use kvproto::{metapb, raft_cmdpb};
use protobuf::{ProtobufEnum, RepeatedField};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use tikv_util::mpsc::Receiver;
use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{error, info, warn};

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
            Task::FinishSplit { region, wait: _ } => write!(
                f,
                "FinishSplit {}:{}",
                region.get_id(),
                region.get_region_epoch().get_version(),
            ),
            Task::RecoverSplit { region, stage, .. } => write!(
                f,
                "recover split for {}, at stage {}",
                RegionIDVer::from_region(region),
                stage.value(),
            ),
            Task::RejectChangeSet { change, .. } => write!(
                f,
                "reject change set for {}",
                RegionIDVer::new(change.get_shard_id(), change.get_shard_ver()),
            ),
        }
    }
}

pub struct Runner {
    kv: kvengine::Engine,
    router: RaftRouter,
    rejects: HashMap<RegionIDVer, kvenginepb::ChangeSet>,
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
        let tp = get_change_set_type(&change_set);
        let id_ver = RegionIDVer::new(change_set.get_shard_id(), change_set.get_shard_ver());
        if let Some(meta_change) = self.rejects.remove(&id_ver) {
            if let Some(shard) = self.kv.get_shard(change_set.get_shard_id()) {
                warn!("shard reject change set";
                    "region" => id_ver,
                    "type" => tp,
                    "seq" => change_set.get_sequence(),
                    "initial_flushed" => shard.get_initial_flushed(),
                );
                if !meta_change.has_compaction() || !meta_change.get_compaction().get_conflicted() {
                    return None;
                }
                change_set.mut_compaction().set_conflicted(true);
                let (tx, rx) = tikv_util::mpsc::bounded(1);
                tx.send(Ok(Task::ApplyChangeSet { change: change_set }));
                return Some(rx);
            }
            warn!("shard not found for prepare change set resource";
                "region" => id_ver, "type" => tp);
            return None;
        }
        info!("shard apply change set"; "region" => id_ver, "type" => tp);
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let res = kv
            .pre_load_files(&change_set)
            .map_err(|err| crate::Error::KVEngineError(err));
        tx.send(res.map(|_| Task::ApplyChangeSet { change: change_set }));
        Some(rx)
    }
}

pub fn get_change_set_type(change_set: &kvenginepb::ChangeSet) -> &'static str {
    if change_set.has_flush() {
        return "flush";
    }
    if change_set.has_compaction() {
        return "compaction";
    }
    if change_set.has_split_files() {
        return "split_files";
    }
    if change_set.has_snapshot() {
        return "snapshot";
    }
    if change_set.has_pre_split() {
        return "pre_split";
    }
    if change_set.has_split() {
        return "split";
    }
    if change_set.next_mem_table_size > 0 {
        return "next_mem_table_size";
    }
    return "unknown";
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, task: Task) {
        let desc = task.to_string();
        match task {
            Task::ApplyChangeSet { change } => {
                if let Some(receiver) = self.prepare_region_resource(change) {
                    self.apply_scheduler.schedule(ApplyTask { desc, receiver });
                }
            }
            Task::RejectChangeSet { change } => {
                let id_ver = RegionIDVer::new(change.get_shard_id(), change.get_shard_ver());
                self.rejects.insert(id_ver, change);
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
                active: false,
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
        let tag = RegionIDVer::from_region(&region);
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
        match task {
            Task::ApplyChangeSet { change } => {
                let id_ver = RegionIDVer::new(change.shard_id, change.shard_ver);
                if let Err(err) = self.handle_apply_change_set(&change) {
                    warn!("failed to apply change set"; "region" => id_ver, "error" => ?err);
                }
            }
            Task::RecoverSplit {
                region,
                peer,
                split_keys,
                stage,
            } => {
                let id_ver = RegionIDVer::from_region(&region);
                if let Err(err) = self.handle_recover_split(region, peer, split_keys, stage) {
                    warn!("failed to handle recover split"; "region" => id_ver, "error" => ?err);
                }
            }
            Task::FinishSplit { region, wait } => {
                let id_ver = RegionIDVer::from_region(&region);
                match finish_split(&self.router, &region, wait.split_keys) {
                    Ok(regions) => {
                        let mut splits = raft_cmdpb::BatchSplitResponse::default();
                        splits.set_regions(RepeatedField::from(regions));
                        let mut admin_resp = raft_cmdpb::AdminResponse::default();
                        admin_resp.set_splits(splits);
                        let mut resp = raft_cmdpb::RaftCmdResponse::default();
                        resp.set_admin_response(admin_resp);
                        wait.callback.invoke_with_response(resp);
                    }
                    Err(err) => {
                        error!("failed to finish split"; "region" => id_ver, "err" => ?err);
                        let resp = err_resp(err, 0);
                        wait.callback.invoke_with_response(resp);
                    }
                }
            }
            Task::RejectChangeSet { .. } => unreachable!(),
        };
    }
}
