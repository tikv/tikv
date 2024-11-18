// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex as SyncMutex},
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
    hooking::{AfterIngestedCtx, BeforeProposeIngestCtx, ImportHook, ImportHookWithInitialize},
    Error::Hooking as HookError,
};
use tikv_util::{box_err, warn, worker::Scheduler};
use tokio::{sync::Mutex, time::Instant};
use txn_types::{Key, TimeStamp};
use uuid::Uuid;

use crate::{
    annotate,
    endpoint::{FetchedTask, TaskOp},
    errors::{ContextualResultExt, Error, Result},
    router::TaskSelector,
    utils, Task,
};

const ROTATE_DURATION: Duration = Duration::from_secs(60);

#[track_caller]
fn convert_err(err: Error) -> sst_importer::Error {
    HookError(box_err!("log backup hooks encounters error: {:?}", err))
}

pub trait Tso {
    fn tso(&self) -> BoxFuture<'_, Result<u64>>;
}

impl<T: PdClient> Tso for Arc<T> {
    fn tso(&self) -> BoxFuture<'_, Result<u64>> {
        PdClient::get_tso(self.as_ref())
            .err_into()
            .map_ok(|v| v.into_inner())
            .boxed()
    }
}

pub struct UploadSst<T> {
    handle: Scheduler<Task>,
    pending_ssts: Mutex<PendingSsts>,
    tso: T,
}

impl<T> UploadSst<T> {
    pub fn new(handle: Scheduler<Task>, tso: T) -> Self {
        Self {
            handle,
            pending_ssts: Mutex::default(),
            tso,
        }
    }

    async fn upload_sst(
        &self,
        cx: BeforeProposeIngestCtx<'_>,
        sst: &SstMeta,
        task: &FetchedTask,
    ) -> Result<brpb::File> {
        let location =
            (cx.locate)(sst).map_err(|err| annotate!(err, "failed to convert SstMeta to path"))?;
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
                .map_err(|err| annotate!(err, "failed to calculate hash"))?
                .to_vec(),
        );
        file.set_size(size);
        file.set_name(save_name_str);
        Ok(file)
    }
}

fn wrap_reader<'a, T: std::io::Read + Send + Sync + 'a>(
    t: T,
) -> Result<(UnpinReader<'a>, Arc<SyncMutex<Hasher>>)> {
    let (rd, hs) =
        Sha256Reader::new(t).map_err(|err| annotate!(err, "failed to create sha256 reader"))?;
    Ok((AllowStdIo::new(rd).into(), hs))
}

fn extract_sst_uuid(meta: &SstMeta) -> Result<Uuid> {
    let uuid_bytes = meta
        .get_uuid()
        .try_into()
        .map_err(|err| annotate!(err, "invalid uuid from {:?}", meta))?;
    let uuid = Uuid::from_bytes(uuid_bytes);
    Ok(uuid)
}

fn lame_file_by(meta: &SstMeta) -> Result<brpb::File> {
    let mut file = brpb::File::default();
    file.set_total_bytes(meta.get_total_bytes());
    file.set_total_kvs(meta.get_total_kvs());
    file.set_cf(meta.get_cf_name().to_owned());
    let decode = |k: &[u8]| {
        Key::from_encoded(k.to_vec())
            .into_raw()
            .map_err(|err| annotate!(err, "cannot decode key {:?}", utils::redact(&k)))
    };
    file.set_start_key(decode(meta.get_range().get_start())?);
    file.set_end_key(decode(meta.get_range().get_end())?);
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
    mig_written: bool,

    _tmp_mig_idx: u64,
}

impl PendingSstsOfTask {
    fn new(task: FetchedTask) -> Self {
        Self {
            committed: Default::default(),
            uncommitted: Default::default(),
            last_update: Instant::now(),
            output_base: None,
            mig_written: false,
            task,

            _tmp_mig_idx: 1,
        }
    }
}

fn initialize_freezer<T: Send + Sync + 'static>(item: &Arc<UploadSst<T>>) {
    let worker = Arc::downgrade(&item);
    let bg_task = async move {
        while let Some(i) = worker.upgrade() {
            {
                let mut l = i.pending_ssts.lock().await;
                for v in l.pending_ssts.values_mut() {
                    if let Err(err) = v.maybe_rotate().await {
                        warn!("failed to rotate sst"; "err" => %err, "task" => %v.task.task.info.name);
                    }
                }
            }
            tokio::time::sleep(ROTATE_DURATION).await;
        }
    };

    tokio::spawn(bg_task);
}

impl PendingSstsOfTask {
    async fn maybe_rotate(&mut self) -> Result<()> {
        if self.should_rotate() {
            self.rotate().await.context("periodically rotate")?;
        }
        Ok(())
    }

    fn push(&mut self, meta: &SstMeta, file: brpb::File) -> Result<()> {
        self.uncommitted.insert(extract_sst_uuid(meta)?, file);
        self.last_update = Instant::now();
        Ok(())
    }

    async fn commit(&mut self, meta: &SstMeta, commit_ts: u64) -> Result<()> {
        if self.should_rotate() {
            self.rotate().await.context("during committing")?;
        }

        let mut af = brpb::AddSstFile::default();
        af.as_if_ts = commit_ts;
        let uuid = extract_sst_uuid(meta)?;
        let file = match self.uncommitted.remove(&uuid) {
            Some(file) => file,
            None => {
                return Err(Error::Other(box_err!(
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

    async fn flush(&mut self) -> Result<()> {
        let flush_to = self
            .output_base
            .as_deref()
            .ok_or_else(|| {
                Error::Other(box_err!(
                    "unreachable: call `flush` when there isn't a target edition"
                ))
            })?
            .to_owned();

        if !self.mig_written {
            let mut mig = brpb::Migration::new();
            mig.mut_add_sst_files().push({
                let mut add_sst_file_ref = brpb::AddSstFilesRef::new();
                add_sst_file_ref.set_path(flush_to.clone());
                add_sst_file_ref
            });
            mig.set_version(brpb::MigrationVersion::AllowNewSsTs);
            let version = format!(
                "tikv;build_hash={};build_branch={}",
                option_env!("TIKV_BUILD_GIT_HASH").unwrap_or("UNKNOWN"),
                option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or("UNKNOWN"),
            );
            mig.mut_creator().push(version);
            self.put_migration(mig).await?;
            self.mig_written = true;
        }

        let bytes = Message::write_to_bytes(&self.committed)
            .map_err(|err| annotate!(err, "cannot marshal sst meta to file"))?;
        self.task
            .storage
            .write(&flush_to, Cursor::new(&bytes).into(), bytes.len() as _)
            .await?;
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        self.output_base.is_none()
            || (self.last_update.elapsed() > ROTATE_DURATION
                && !self.committed.add_sst_file.is_empty())
    }

    fn gen_commit_to_path(&self) -> String {
        format!("ingested/committing-{:08X}", TimeStamp::physical_now())
    }

    async fn rotate(&mut self) -> Result<()> {
        if self.output_base.is_some() {
            self.committed.forzen = true;
            self.flush().await.context("initial rotate")?;
        }

        let commit_to = self.gen_commit_to_path();

        self.output_base = Some(commit_to);
        self.committed = Default::default();
        self.mig_written = false;
        Ok(())
    }

    async fn put_migration(&mut self, mig: brpb::Migration) -> Result<()> {
        // TODO: use the real Migration Toolkit.
        let bytes = mig
            .write_to_bytes()
            .context(format!("failed to marshal the migration {:?}", mig))?;
        self._tmp_mig_idx += 1;
        self.task
            .storage
            .write(
                &format!(
                    "v1/migrations/{:08}_{:016X}.mgrt",
                    self._tmp_mig_idx,
                    rand::random::<u64>()
                ),
                Cursor::new(&bytes).into(),
                bytes.len() as _,
            )
            .await?;
        Ok(())
    }
}

impl PendingSsts {
    fn insert(&mut self, meta: &SstMeta, task: &FetchedTask, file: brpb::File) -> Result<()> {
        let name = &task.task.info.name;
        let Some(ssts) = self.pending_ssts.get_mut(name) else {
            warn!("Try to insert a stale SST (The task was already removed), skipping."; "sst" => ?meta, "task" => %task.task.info.name);
            return Ok(());
        };
        ssts.push(meta, file)?;
        Ok(())
    }

    async fn sync_tasks(&mut self, log_backup_hnd: &Scheduler<Task>) -> Result<Vec<FetchedTask>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        log_backup_hnd.schedule(Task::LogBackupTask(TaskOp::Query(TaskSelector::All, tx)))?;
        let tasks = rx
            .await
            .map_err(|err| annotate!(err, "request dropped, maybe shutting down"))?;
        assert!(tasks.len() <= 1);
        self.sync_tasks_with(tasks.iter());
        Ok(tasks)
    }

    fn sync_tasks_with<'a>(&mut self, tasks: impl IntoIterator<Item = &'a FetchedTask>) {
        let mut names = HashSet::new();

        for task in tasks {
            let name = &task.task.info.name;
            names.insert(name);
            if !self.pending_ssts.contains_key(name) {
                self.pending_ssts
                    .insert(name.clone(), PendingSstsOfTask::new(task.clone()));
            }
        }

        self.pending_ssts.retain(|k, _| names.contains(k));
    }
}

impl<T: Tso + Send + Sync + 'static> ImportHook for UploadSst<T> {
    fn before_propose_ingest<'a: 'r, 'b: 'r, 'r>(
        &'a self,
        cx: BeforeProposeIngestCtx<'b>,
    ) -> BoxFuture<'r, sst_importer::Result<()>> {
        async move {
            let tasks = self
                .pending_ssts
                .lock()
                .await
                .sync_tasks(&self.handle)
                .await
                .map_err(convert_err)?;

            for task in tasks {
                for sst_meta in cx.sst_meta.iter() {
                    let overlapping = task.ranges.iter().any(|(start, end)| {
                        utils::is_overlapping(
                            (&start, &end),
                            (
                                sst_meta.get_range().get_start(),
                                sst_meta.get_range().get_end(),
                            ),
                        )
                    });
                    if !overlapping {
                        continue;
                    }
                    let file = self
                        .upload_sst(cx, sst_meta, &task)
                        .await
                        .map_err(convert_err)?;
                    self.pending_ssts
                        .lock()
                        .await
                        .insert(&sst_meta, &task, file)
                        .map_err(convert_err)?;
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
                    let fut = self.tso.tso().and_then(|tso| task.commit(sst, tso));
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

impl<T: Send + Sync + 'static> ImportHookWithInitialize for UploadSst<T> {
    fn init(self: Arc<Self>, tokio: &tokio::runtime::Handle) {
        let _rt = tokio.enter();
        initialize_freezer(&self);
    }
}
