// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use chrono::Local;
pub use engine_traits::SstCompressionType;
use external_storage::{locking::RemoteLock, ExternalStorage, UnpinReader};
use futures::{future::TryFutureExt, io::Cursor};
use kvproto::brpb;
use tikv_util::{
    info,
    logger::{get_log_level, Level},
    stream::{retry, JustRetry},
    warn,
};
use tokio::{io::AsyncWriteExt, runtime::Handle, signal::unix::SignalKind};

use crate::{
    compaction::{
        meta::CompactionRunInfoBuilder, Subcompaction, SubcompactionResult, META_OUT_REL,
        SST_OUT_REL,
    },
    errors::Result,
    execute::Execution,
    statistic::{
        CollectSubcompactionStatistic, CompactLogBackupStatistic, LoadMetaStatistic, LoadStatistic,
        SubcompactStatistic,
    },
    storage::{StreamyMetaStorage, LOCK_PREFIX},
    util, Error,
};

pub struct NoHooks;

impl ExecHooks for NoHooks {}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct CId(pub u64);

impl std::fmt::Display for CId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy)]
pub struct BeforeStartCtx<'a> {
    /// The asynchronous runtime that we are about to use.
    pub async_rt: &'a Handle,
    /// Reference to the execution context.
    pub this: &'a Execution,
    /// The source external storage of this compaction.
    pub storage: &'a dyn ExternalStorage,
}

#[derive(Clone, Copy)]
pub struct AfterFinishCtx<'a> {
    /// The asynchronous runtime that we are about to use.
    pub async_rt: &'a Handle,
    /// The target external storage of this compaction.
    ///
    /// For now, it is always the same as the source storage.
    pub storage: &'a dyn ExternalStorage,
}

#[derive(Clone, Copy)]
pub struct SubcompactionFinishCtx<'a> {
    /// Reference to the execution context.
    pub this: &'a Execution,
    /// The target external storage of this compaction.
    pub external_storage: &'a dyn ExternalStorage,
    /// The result of this compaction.
    ///
    /// If this is an `Err`, the whole procedure may fail soon.
    pub result: &'a SubcompactionResult,
}

#[derive(Clone, Copy)]
pub struct SubcompactionStartCtx<'a> {
    /// The subcompaction about to start.
    pub subc: &'a Subcompaction,
    /// The diff of statistic of loading metadata.
    ///
    /// The diff is between the last trigger of `before_a_subcompaction_start`
    /// and now. Due to we will usually prefetch metadata, this diff may not
    /// just contributed by the `subc` only.
    pub load_stat_diff: &'a LoadMetaStatistic,
    /// The diff of collecting compaction between last trigger of this event.
    ///
    /// Like `load_stat_diff`, we collect subcompactions for every region
    /// concurrently. The statistic diff may not just contributed by the `subc`.
    pub collect_compaction_stat_diff: &'a CollectSubcompactionStatistic,
}

#[derive(Clone, Copy)]
pub struct AbortedCtx<'a> {
    pub storage: &'a dyn ExternalStorage,
    pub err: &'a Error,
}

/// The hook points of an execution of compaction.
// We don't need the returned future be either `Send` or `Sync`.
#[allow(async_fn_in_trait)]
#[allow(clippy::unused_async)]
pub trait ExecHooks: 'static {
    /// This hook will be called when a subcompaction is about to start.
    fn before_a_subcompaction_start(&mut self, _cid: CId, _c: SubcompactionStartCtx<'_>) {}
    /// This hook will be called when a subcompaction has been finished.
    /// You may use the `cid` to match a subcompaction previously known by
    /// [`ExecHooks::before_a_subcompaction_start`].
    ///
    /// If an error was returned, the whole procedure will fail and be
    /// terminated!
    async fn after_a_subcompaction_end(
        &mut self,
        _cid: CId,
        _res: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        Ok(())
    }

    /// This hook will be called before all works begin.
    /// In this time, the asynchronous runtime and external storage have been
    /// created.
    ///
    /// If an error was returned, the execution will be aborted.
    async fn before_execution_started(&mut self, _cx: BeforeStartCtx<'_>) -> Result<()> {
        Ok(())
    }
    /// This hook will be called after the whole compaction finished.
    ///
    /// If an error was returned, the execution will be mark as failed.
    async fn after_execution_finished(&mut self, _cx: AfterFinishCtx<'_>) -> Result<()> {
        Ok(())
    }

    async fn on_aborted(&mut self, _cx: AbortedCtx<'_>) {}
}

impl<T: ExecHooks, U: ExecHooks> ExecHooks for (T, U) {
    fn before_a_subcompaction_start(&mut self, cid: CId, c: SubcompactionStartCtx<'_>) {
        self.0.before_a_subcompaction_start(cid, c);
        self.1.before_a_subcompaction_start(cid, c);
    }

    async fn after_a_subcompaction_end(
        &mut self,
        cid: CId,
        cx: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        futures::future::try_join(
            self.0.after_a_subcompaction_end(cid, cx),
            self.1.after_a_subcompaction_end(cid, cx),
        )
        .await?;
        Ok(())
    }

    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        futures::future::try_join(
            self.0.before_execution_started(cx),
            self.1.before_execution_started(cx),
        )
        .await?;
        Ok(())
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        futures::future::try_join(
            self.0.after_execution_finished(cx),
            self.1.after_execution_finished(cx),
        )
        .await?;
        Ok(())
    }

    async fn on_aborted(&mut self, cx: AbortedCtx<'_>) {
        futures::future::join(self.0.on_aborted(cx), self.1.on_aborted(cx)).await;
    }
}

/// The hooks that used for an execution from a TTY.
///
/// This prints the log when events happens, and prints statistics after
/// compaction finished.
///
/// This also enables async-backtrace, you can send `SIGUSR1` to the executing
/// compaction task and the running async tasks will be dumped to a file.
#[derive(Default)]
pub struct TuiHooks {
    stats: CollectStatistic,
    meta_len: u64,
}

impl ExecHooks for TuiHooks {
    fn before_a_subcompaction_start(&mut self, cid: CId, cx: SubcompactionStartCtx<'_>) {
        let c = cx.subc;
        self.stats
            .update_collect_compaction_stat(cx.collect_compaction_stat_diff);
        self.stats.update_load_meta_stat(cx.load_stat_diff);

        let level = get_log_level();
        if level < Some(Level::Info) {
            warn!("Most of compact-log progress logs are only enabled in the `info` level."; "current_level" => ?level);
        }

        info!("Spawning compaction."; "cid" => cid.0, 
            "cf" => c.cf, 
            "input_min_ts" => c.input_min_ts, 
            "input_max_ts" => c.input_max_ts, 
            "source" => c.inputs.len(), 
            "size" => c.size, 
            "region_id" => c.region_id);
    }

    async fn after_a_subcompaction_end(
        &mut self,
        cid: CId,
        cx: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        let lst = &cx.result.load_stat;
        let cst = &cx.result.compact_stat;
        let logical_input_size = lst.logical_key_bytes_in + lst.logical_value_bytes_in;
        let total_take =
            cst.load_duration + cst.sort_duration + cst.save_duration + cst.write_sst_duration;
        let speed = logical_input_size as f64 / total_take.as_millis() as f64;
        self.stats.update_subcompaction(cx.result);

        info!("Finishing compaction."; "meta_completed" => self.stats.load_meta_stat.meta_files_in, 
            "meta_total" => self.meta_len, 
            "p_bytes" => self.stats.collect_stat.bytes_in - self.stats.collect_stat.bytes_out, 
            "cid" => cid.0, 
            "load_stat" => ?lst, 
            "compact_stat" => ?cst, 
            "speed(KiB/s)" => speed, 
            "total_take" => ?total_take, 
            "global_load_meta_stat" => ?self.stats.load_meta_stat);
        Ok(())
    }

    async fn after_execution_finished(&mut self, _cx: AfterFinishCtx<'_>) -> Result<()> {
        info!("All compactions done.");
        Ok(())
    }

    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        tracing_active_tree::init();

        let sigusr1_handler = async {
            let mut signal = tokio::signal::unix::signal(SignalKind::user_defined1()).unwrap();
            while signal.recv().await.is_some() {
                let file_name = "/tmp/compact-sst.dump".to_owned();
                let res = async {
                    let mut file = tokio::fs::File::create(&file_name).await?;
                    file.write_all(&tracing_active_tree::layer::global().fmt_bytes())
                        .await
                }
                .await;
                match res {
                    Ok(_) => warn!("dumped async backtrace."; "to" => file_name),
                    Err(err) => warn!("failed to dump async backtrace."; "err" => %err),
                }
            }
        };

        cx.async_rt.spawn(sigusr1_handler);
        self.meta_len = StreamyMetaStorage::count_objects(cx.storage).await?;
        Ok(())
    }
}

#[derive(Default)]
struct CollectStatistic {
    load_stat: LoadStatistic,
    compact_stat: SubcompactStatistic,
    load_meta_stat: LoadMetaStatistic,
    collect_stat: CollectSubcompactionStatistic,
}

impl CollectStatistic {
    fn update_subcompaction(&mut self, res: &SubcompactionResult) {
        self.load_stat += res.load_stat.clone();
        self.compact_stat += res.compact_stat.clone();
    }

    fn update_collect_compaction_stat(&mut self, stat: &CollectSubcompactionStatistic) {
        self.collect_stat += stat.clone()
    }

    fn update_load_meta_stat(&mut self, stat: &LoadMetaStatistic) {
        self.load_meta_stat += stat.clone()
    }
}

/// Save the metadata to external storage after every subcompaction. After
/// everything done, it saves the whole compaction to a "migration" that can be
/// read by the BR CLI.
///
/// This is an essential plugin for real-world compacting, as single SST cannot
/// be restored.
///
/// "But why not just save the metadata of compaction in
/// [`SubcompactionExec`](crate::compaction::exec::SubcompactionExec)?"
///
/// First, As the hook system isn't exposed to end user, whether inlining this
/// is transparent to them -- they won't mistakely forget to add this hook and
/// ruin everything.
///
/// Also this make `SubcompactionExec` standalone, it will be easier to test.
///
/// The most important is, the hook knows metadata crossing subcompactions,
/// we can then optimize the arrangement of subcompactions (say, batching
/// subcompactoins), and save the final result in a single migration.
/// While [`SubcompactionExec`](crate::compaction::exec::SubcompactionExec)
/// knows only the subcompaction it handles, it is impossible to do such
/// optimizations.
pub struct SaveMeta {
    collector: CompactionRunInfoBuilder,
    stats: CollectStatistic,
    begin: chrono::DateTime<Local>,
}

impl Default for SaveMeta {
    fn default() -> Self {
        Self {
            collector: Default::default(),
            stats: Default::default(),
            begin: Local::now(),
        }
    }
}

impl SaveMeta {
    fn comments(&self) -> String {
        let now = Local::now();
        let stat = CompactLogBackupStatistic {
            start_time: self.begin,
            end_time: Local::now(),
            time_taken: (now - self.begin).to_std().unwrap_or_default(),
            exec_by: tikv_util::sys::hostname().unwrap_or_default(),

            load_stat: self.stats.load_stat.clone(),
            subcompact_stat: self.stats.compact_stat.clone(),
            load_meta_stat: self.stats.load_meta_stat.clone(),
            collect_subcompactions_stat: self.stats.collect_stat.clone(),
            prometheus: Default::default(),
        };
        serde_json::to_string(&stat).unwrap_or_else(|err| format!("ERR DURING MARSHALING: {}", err))
    }
}

impl ExecHooks for SaveMeta {
    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        self.begin = Local::now();
        let run_info = &mut self.collector;
        run_info.mut_meta().set_name(cx.this.gen_name());
        run_info
            .mut_meta()
            .set_compaction_from_ts(cx.this.cfg.from_ts);
        run_info
            .mut_meta()
            .set_compaction_until_ts(cx.this.cfg.until_ts);
        run_info
            .mut_meta()
            .set_artifacts(format!("{}/{}", cx.this.out_prefix, META_OUT_REL));
        run_info
            .mut_meta()
            .set_generated_files(format!("{}/{}", cx.this.out_prefix, SST_OUT_REL));
        Ok(())
    }

    fn before_a_subcompaction_start(&mut self, _cid: CId, c: SubcompactionStartCtx<'_>) {
        self.stats
            .update_collect_compaction_stat(c.collect_compaction_stat_diff);
        self.stats.update_load_meta_stat(c.load_stat_diff);
    }

    async fn after_a_subcompaction_end(
        &mut self,
        _cid: CId,
        cx: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        use protobuf::Message;

        self.collector.add_subcompaction(cx.result);
        self.stats.update_subcompaction(cx.result);

        let meta_name = format!(
            "{}_{}_{}.cmeta",
            util::aligned_u64(cx.result.origin.input_min_ts),
            util::aligned_u64(cx.result.origin.input_max_ts),
            util::aligned_u64(cx.result.origin.crc64())
        );
        let meta_name = format!("{}/{}/{}", cx.this.out_prefix, META_OUT_REL, meta_name);
        let mut metas = brpb::LogFileSubcompactions::new();
        metas.mut_subcompactions().push(cx.result.meta.clone());
        let meta_bytes = metas.write_to_bytes()?;
        retry(|| async {
            let reader = UnpinReader(Box::new(Cursor::new(&meta_bytes)));
            cx.external_storage
                .write(&meta_name, reader, meta_bytes.len() as _)
                .map_err(JustRetry)
                .await
        })
        .await
        .map_err(|err| err.0)?;
        Result::Ok(())
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        let comments = self.comments();
        self.collector.mut_meta().set_comments(comments);
        self.collector.write_migration(cx.storage).await
    }
}

#[derive(Default)]
pub struct WithLock {
    lock: Option<RemoteLock>,
}

impl ExecHooks for WithLock {
    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        use external_storage::locking::LockExt;

        let hint = format!(
            "This is generated by the compaction {}.",
            cx.this.gen_name()
        );
        self.lock = Some(cx.storage.lock_for_read(LOCK_PREFIX, hint).await?);

        Ok(())
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        if let Some(lock) = self.lock.take() {
            lock.unlock(cx.storage).await?;
        }
        Ok(())
    }

    async fn on_aborted(&mut self, cx: AbortedCtx<'_>) {
        if let Some(lock) = self.lock.take() {
            warn!("It seems compaction failed. Resolving the lock."; "err" => ?cx.err);
            if let Err(err) = lock.unlock(cx.storage).await {
                warn!("Failed to unlock when failed, you may resolve the lock manually"; "err" => ?err, "lock" => ?lock);
            }
        }
    }
}
