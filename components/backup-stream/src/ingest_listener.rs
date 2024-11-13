// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt::Display,
    future::ready,
    path::Path,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use external_storage::UnpinReader;
use file_system::{File, Sha256Reader};
use futures::{
    future::{FutureExt, TryFutureExt},
    io::{AllowStdIo, Cursor},
    prelude::future::BoxFuture,
};
use kvproto::{brpb, import_sstpb::SstMeta};
use openssl::hash::Hasher;
use pd_client::PdClient;
use protobuf::Message;
use sst_importer::{
    hooking::{AfterIngestedCtx, BeforeProposeIngestCtx, ImportHook},
    Error::Hooking as HookError,
};
use tikv_util::{box_err, info, warn, worker::Scheduler};
use tokio::time::Instant;
use txn_types::{Key, TimeStamp};
use uuid::Uuid;

use crate::{
    annotate,
    endpoint::{FetchedTask, TaskOp},
    router::TaskSelector,
    utils, Task,
};

pub trait Tso {
    fn tso(&self) -> BoxFuture<'_, crate::errors::Result<u64>>;
}

impl<T: PdClient> Tso for Arc<T> {
    fn tso(&self) -> BoxFuture<'_, crate::errors::Result<u64>> {
        PdClient::get_tso(self.as_ref())
            .err_into()
            .map_ok(|v| v.into_inner())
            .boxed()
    }
}

pub struct UploadSst<T> {
    handle: Scheduler<Task>,
    pending_ssts: tokio::sync::Mutex<PendingSsts>,
    tso: T,
}

impl<T> UploadSst<T> {
    pub fn new(handle: Scheduler<Task>, tso: T) -> Self {
        Self {
            handle,
            pending_ssts: tokio::sync::Mutex::default(),
            tso,
        }
    }
}

fn wrap_reader<'a, T: std::io::Read + Send + Sync + 'a>(
    t: T,
) -> sst_importer::Result<(UnpinReader<'a>, Arc<Mutex<Hasher>>)> {
    let (rd, hs) =
        Sha256Reader::new(t).map_err(|err| annotate(err, "failed to create sha256 reader"))?;
    Ok((AllowStdIo::new(rd).into(), hs))
}

#[track_caller]
fn annotate<Err: Display>(err: Err, annotation: impl Display) -> sst_importer::Error {
    HookError(box_err!("{}: {}", annotation, err))
}

fn extract_sst_uuid(meta: &SstMeta) -> sst_importer::Result<Uuid> {
    let uuid_bytes = meta
        .get_uuid()
        .try_into()
        .map_err(|err| annotate(err, "invalid uuid"))?;
    let uuid = Uuid::from_bytes(uuid_bytes);
    Ok(uuid)
}

fn lame_file_by(meta: &SstMeta) -> sst_importer::Result<brpb::File> {
    let mut file = brpb::File::default();
    file.set_total_bytes(meta.get_total_bytes());
    file.set_total_kvs(meta.get_total_kvs());
    file.set_cf(meta.get_cf_name().to_owned());
    let decode = |k: Vec<u8>| Key::from_encoded(k.clone()).into_raw();
    file.set_start_key(decode(meta.get_range().get_start().to_vec())?);
    file.set_end_key(decode(meta.get_range().get_end().to_vec())?);
    if !meta.end_key_exclusive {
        file.mut_end_key().push(0);
    }

    Ok(file)
}

#[derive(Default)]
struct PendingSsts {
    pending_ssts: HashMap<String, PendingSstsOfTask>,
}

struct PendingSstsOfTask {
    committed: brpb::AddSstFiles,
    uncommitted: HashMap<Uuid, brpb::File>,
    task: FetchedTask,

    last_update: Instant,
    output_base: Option<String>,
}

impl PendingSstsOfTask {
    fn new(task: FetchedTask) -> Self {
        Self {
            committed: Default::default(),
            uncommitted: Default::default(),
            last_update: Instant::now(),
            output_base: None,
            task,
        }
    }
}

impl PendingSstsOfTask {
    fn push(&mut self, meta: &SstMeta, file: brpb::File) -> sst_importer::Result<()> {
        self.uncommitted.insert(extract_sst_uuid(meta)?, file);
        self.last_update = Instant::now();
        Ok(())
    }

    async fn commit(&mut self, meta: &SstMeta, commit_ts: u64) -> sst_importer::Result<()> {
        if self.should_rotate() {
            self.rotate().await?;
        }

        let mut af = brpb::AddSstFile::default();
        af.as_if_ts = commit_ts;
        let uuid = extract_sst_uuid(meta)?;
        let file = match self.uncommitted.remove(&uuid) {
            Some(file) => file,
            None => {
                return Err(HookError(box_err!(
                    "cannot find the sst meta with uuid {}",
                    uuid
                )));
            }
        };
        af.set_file(file);

        self.committed.add_sst_file.push(af);
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> sst_importer::Result<()> {
        let flush_to = self
            .output_base
            .as_deref()
            .ok_or_else(|| HookError(box_err!("call `flush` when there isn't a target edition")))?;
        let bytes = Message::write_to_bytes(&self.committed)
            .map_err(|err| annotate(err, "cannot marshal sst meta to file"))?;
        self.task
            .storage
            .write(flush_to, Cursor::new(&bytes).into(), bytes.len() as _)
            .await?;
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        self.output_base.is_none() || self.last_update.elapsed() > Duration::from_secs(60)
    }

    fn gen_commit_to_path(&self) -> String {
        format!("ingested/committing-{:08X}", TimeStamp::physical_now())
    }

    async fn rotate(&mut self) -> sst_importer::Result<()> {
        if self.output_base.is_some() {
            self.committed.forzen = true;
            self.flush().await?;
        }

        let commit_to = self.gen_commit_to_path();

        let mut mig = brpb::Migration::new();
        mig.mut_add_sst_files().push({
            let mut add_sst_file_ref = brpb::AddSstFilesRef::new();
            add_sst_file_ref.set_path(commit_to.clone());
            add_sst_file_ref
        });
        self.put_migration(mig).await?;

        self.output_base = Some(commit_to);
        Ok(())
    }

    async fn put_migration(&self, mig: brpb::Migration) -> sst_importer::Result<()> {
        // TODO: use the real Migration Toolkit.
        let bytes = mig
            .write_to_bytes()
            .map_err(|err| annotate(err, "cannot marshal sst meta to file"))?;
        self.task
            .storage
            .write(
                &format!("v1/migrations/{:08}_{:16X}.mgrt", 1, rand::random::<u64>()),
                Cursor::new(&bytes).into(),
                bytes.len() as _,
            )
            .await?;
        Ok(())
    }
}

impl PendingSsts {
    fn insert(
        &mut self,
        meta: &SstMeta,
        task: &FetchedTask,
        file: brpb::File,
    ) -> sst_importer::Result<()> {
        let name = &task.task.info.name;
        if !self.pending_ssts.contains_key(name) {
            self.pending_ssts
                .insert(name.clone(), PendingSstsOfTask::new(task.clone()));
        }

        let ssts = self.pending_ssts.get_mut(name).unwrap();
        ssts.push(meta, file)?;
        Ok(())
    }
}

impl<T: Tso + Send + Sync + 'static> ImportHook for UploadSst<T> {
    fn before_propose_ingest<'a: 'r, 'b: 'r, 'r>(
        &'a self,
        cx: BeforeProposeIngestCtx<'b>,
    ) -> BoxFuture<'r, sst_importer::Result<()>> {
        async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.handle
                .schedule(Task::LogBackupTask(TaskOp::Query(TaskSelector::All, tx)))
                .map_err(|err| {
                    annotate(err, "query log backup endpoint failed, maybe shutting down")
                })?;
            let tasks = rx
                .await
                .map_err(|err| annotate(err, "query channel dropped, maybe shutting down"))?;
            assert!(tasks.len() <= 1);

            for task in tasks {
                // NOTE: We may need to check whether the range overlaps. Or we may backup
                // redundant data. For now it is OK because all tasks covers the
                // whole TiDB user keyspace.

                for sst_meta in cx.sst_meta.iter() {
                    let location = (cx.locate)(sst_meta)
                        .map_err(|err| annotate(err, "failed to convert SstMeta to path"))?;
                    let sst_meta = location.meta;
                    let name = location.local_path;
                    let save_name = format!(
                        "{:016X}_{:016X}_{}.sst",
                        sst_meta.region_id,
                        sst_meta.get_region_epoch().version,
                        hex::encode(sst_meta.get_uuid())
                    );
                    let save_path = Path::new("ingested")
                        .join(format!("{:016X}", TimeStamp::physical_now() / 60000))
                        .join(save_name);
                    let save_name_str = save_path.display().to_string();
                    let reader = File::open(&name)?;
                    let size = reader.metadata()?.len();
                    let (reader, hasher) = match cx.key_manager {
                        None => wrap_reader(reader)?,
                        Some(ref km) => wrap_reader(km.open_file_with_reader(name, reader)?)?,
                    };

                    let mut file = lame_file_by(&sst_meta)?;
                    task.storage.write(&save_name_str, reader, size).await?;
                    file.set_sha256(
                        hasher
                            .lock()
                            .unwrap()
                            .finish()
                            .map_err(|err| annotate(err, "failed to calculate hash"))?
                            .to_vec(),
                    );
                    file.set_size(size);
                    file.set_name(save_name_str);

                    self.pending_ssts
                        .lock()
                        .await
                        .insert(&sst_meta, &task, file)?;
                }
            }
            Ok(())
        }
        .boxed()
    }

    fn after_ingested<'a: 'r, 'b: 'r, 'r>(&'a self, cx: AfterIngestedCtx<'b>) -> BoxFuture<'r, ()> {
        async move {
            let mut l = self.pending_ssts.lock().await;
            for task in l.pending_ssts.values_mut() {
                for sst in cx.sst_meta {
                    let fut = self.tso.tso().map_err(|err| annotate(err, "failed to fetch tso")).and_then(|tso| task.commit(sst, tso));
                    if let Err(err) = fut.await {
                        let task_name = &task.task.task.info.name;
                        warn!("failed to commit sst"; "err" => %err, "sst" => ?sst, "task" => %task_name);
                        // Failed to send should only happen when we are shutting down, which is acceptable.
                        let _ = self.handle.schedule_force(Task::FatalError(TaskSelector::ByName(task_name.clone()), Box::new(annotate!(err, "failed to commit ingested SST"))));
                    }
                }
            }
        }
        .boxed()
    }
}
