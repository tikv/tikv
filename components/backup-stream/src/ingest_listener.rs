// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fmt::Display, future::ready, path::Path, sync::Mutex};

use dashmap::DashMap;
use file_system::File;
use futures::{future::FutureExt, io::AllowStdIo, prelude::future::BoxFuture};
use kvproto::import_sstpb::SstMeta;
use sst_importer::{
    hooking::{AfterIngestedCtx, BeforeProposeIngestCtx, ImportHook},
    Error::Hooking as HookError,
};
use tikv_util::{box_err, warn, worker::Scheduler};
use uuid::Uuid;

use crate::{
    annotate,
    endpoint::{FetchedTask, TaskOp},
    router::TaskSelector,
    Task,
};

pub struct UploadSst {
    handle: Scheduler<Task>,
    pending_ssts: Mutex<PendingSsts>,
}

impl UploadSst {
    pub fn new(handle: Scheduler<Task>) -> Self {
        Self {
            handle,
            pending_ssts: Mutex::new(PendingSsts {
                pending_ssts: HashMap::new(),
            }),
        }
    }
}

fn annotate<Err: Display>(err: Err, annotation: impl Display) -> sst_importer::Error {
    HookError(box_err!("{}: {}", annotation, err))
}

#[derive(Hash, Eq, PartialEq)]
struct PendingSstKey {
    sst_id: Uuid,
}

impl PendingSstKey {
    pub fn new(meta: &SstMeta) -> sst_importer::Result<Self> {
        let uuid_bytes = meta
            .get_uuid()
            .try_into()
            .map_err(|err| annotate(err, "invalid uuid"))?;
        let uuid = Uuid::from_bytes(uuid_bytes);
        let result = Self { sst_id: uuid };
        Ok(result)
    }
}

struct PendingSsts {
    pending_ssts: HashMap<PendingSstKey, (SstMeta, Vec<FetchedTask>)>,
}

impl PendingSsts {
    fn insert(&mut self, meta: &SstMeta, task: &FetchedTask) -> sst_importer::Result<()> {
        let key = PendingSstKey::new(meta)?;
        let (_, tasks) = self
            .pending_ssts
            .entry(key)
            .or_insert_with(|| (meta.clone(), vec![]));
        tasks.push(task.clone());

        Ok(())
    }

    fn tasks_of(&mut self, meta: &SstMeta) -> sst_importer::Result<Vec<FetchedTask>> {
        let key = PendingSstKey::new(&meta)?;
        let (_, fetched_tasks) = self
            .pending_ssts
            .remove(&key)
            .ok_or_else(|| HookError(box_err!("committing a SST not uploaded: {:?}", meta)))?;
        Ok(fetched_tasks)
    }
}

impl ImportHook for UploadSst {
    fn before_propose_ingest(
        &self,
        cx: BeforeProposeIngestCtx<'_>,
    ) -> BoxFuture<'_, sst_importer::Result<()>> {
        let ssts = cx
            .sst_meta
            .iter()
            .map(|m| (m.clone(), (cx.sst_to_path)(m)))
            .collect::<Vec<_>>();
        async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self
                .handle
                // TODO: Should use range instead of select all.
                .schedule(Task::LogBackupTask(TaskOp::Query(TaskSelector::All, tx)))
                .map_err(|err| annotate(err, "query log backup endpoint failed, maybe shutting down"))?;
            let tasks = rx.await
                .map_err(|err| annotate(err, "query channel dropped, maybe shutting down"))?;
            assert!(tasks.len() <= 1);

            for task in tasks {
                for (sst_meta, name) in ssts.iter() {
                    let name = name.as_ref().map_err(|err| annotate(err, "failed to convert SstMeta to path"))?;
                    let save_name = format!("{:08X}_{:08X}_{}.sst", sst_meta.region_id, sst_meta.get_region_epoch().version, hex::encode(sst_meta.get_uuid()));
                    let save_path = Path::new("ssts").join(save_name);
                    let reader = File::open(name)?;
                    let size = reader.metadata()?.len();
                    task.storage.write(&save_path.display().to_string(), AllowStdIo::new(reader).into(), size).await?;
                    self.pending_ssts.lock().unwrap().insert(sst_meta, &task)?;
                }
            }
            Ok(())
        }
        .boxed()
    }

    fn after_ingested(&self, cx: AfterIngestedCtx<'_>) -> BoxFuture<'_, ()> {
        let maybe_to_commit = (|| {
            let mut to_commit = vec![];
            for meta in cx.sst_meta {
                let mut pend_ssts = self.pending_ssts.lock().unwrap();
                let tasks = pend_ssts.tasks_of(meta)?;
                to_commit.push((meta.clone(), tasks));
            }
            sst_importer::Result::Ok(to_commit)
        })();

        async move {
            let to_commit = maybe_to_commit?;
            for (meta, fetched_task) in to_commit {
                for task in fetched_task {
                    let name = format!("commit_sst/{}", hex::encode(meta.get_uuid()));
                    task.storage
                        .write(&name, futures::io::empty().into(), 0)
                        .await?;
                }
            }
            sst_importer::Result::Ok(())
        }
        .map(|v| match v {
            Ok(()) => (),
            // TODO: issue a fatal error.
            Err(err) => warn!("encountered an error during committing sst"; "err" => %err),
        })
        .boxed()
    }
}
