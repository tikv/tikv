// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::future::Future;

use chrono::{Duration, Local};
pub use engine_traits::SstCompressionType;
use external_storage::{FullFeaturedStorage, UnpinReader};
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
    statistic::{CollectCompactionStatistic, CompactStatistic, LoadMetaStatistic, LoadStatistic},
    util,
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
    pub async_rt: &'a Handle,
    pub est_meta_size: u64,
    pub this: &'a Execution,
}

#[derive(Clone, Copy)]
pub struct AfterFinishCtx<'a> {
    pub async_rt: &'a Handle,
    pub external_storage: &'a dyn FullFeaturedStorage,
}

#[derive(Clone, Copy)]
pub struct SubcompactionFinishCtx<'a> {
    pub this: &'a Execution,
    pub external_storage: &'a dyn FullFeaturedStorage,
    pub result: &'a SubcompactionResult,
}

#[derive(Clone, Copy)]
pub struct SubcompactionStartCtx<'a> {
    pub subc: &'a Subcompaction,
    pub load_stat_diff: &'a LoadMetaStatistic,
    pub collect_compaction_stat_diff: &'a CollectCompactionStatistic,
}

/// The hook points of an execution of compaction.
pub trait ExecHooks: 'static {
    /// This hook will be called when a subcompaction is about to start.
    fn before_a_subcompaction_start(&mut self, _cid: CId, _c: SubcompactionStartCtx<'_>) {}
    /// This hook will be called when a subcompaction has been finished.
    /// You may use the `cid` to match a subcompaction previously known by
    /// [`ExecHooks::before_a_subcompaction_start`].
    fn after_a_subcompaction_end<'a>(
        &'a mut self,
        _cid: CId,
        _res: SubcompactionFinishCtx<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        futures::future::ok(())
    }

    /// This hook will be called before all works begin.
    /// In this time, the asynchronous runtime and external storage have been
    /// created.
    fn before_execution_started(&mut self, _cx: BeforeStartCtx<'_>) {}
    /// This hook will be called after the whole compaction finished.
    fn after_execution_finished<'a>(
        &'a mut self,
        _cx: AfterFinishCtx<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        futures::future::ok(())
    }
}

impl<T: ExecHooks, U: ExecHooks> ExecHooks for (T, U) {
    fn before_a_subcompaction_start(&mut self, cid: CId, c: SubcompactionStartCtx<'_>) {
        self.0.before_a_subcompaction_start(cid, c);
        self.1.before_a_subcompaction_start(cid, c);
    }

    async fn after_a_subcompaction_end<'a>(
        &'a mut self,
        cid: CId,
        cx: SubcompactionFinishCtx<'a>,
    ) -> Result<()> {
        futures::future::try_join(
            self.0.after_a_subcompaction_end(cid, cx),
            self.1.after_a_subcompaction_end(cid, cx),
        )
        .await?;
        Ok(())
    }

    fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) {
        self.0.before_execution_started(cx);
        self.1.before_execution_started(cx);
    }

    async fn after_execution_finished<'a>(&'a mut self, cx: AfterFinishCtx<'a>) -> Result<()> {
        futures::future::try_join(
            self.0.after_execution_finished(cx),
            self.1.after_execution_finished(cx),
        )
        .await?;
        Ok(())
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

    fn after_a_subcompaction_end<'a>(
        &'a mut self,
        cid: CId,
        cx: SubcompactionFinishCtx<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        let lst = &cx.result.load_stat;
        let cst = &cx.result.compact_stat;
        let logical_input_size = lst.logical_key_bytes_in + lst.logical_value_bytes_in;
        let total_take =
            cst.load_duration + cst.sort_duration + cst.save_duration + cst.write_sst_duration;
        let speed = logical_input_size as f64 / total_take.as_millis() as f64;
        self.stats.update_subcompaction(&cx.result);

        info!("Finishing compaction."; "meta_completed" => self.stats.load_meta_stat.meta_files_in, 
            "meta_total" => self.meta_len, 
            "p_bytes" => self.stats.collect_stat.bytes_in - self.stats.collect_stat.bytes_out, 
            "cid" => cid.0, 
            "load_stat" => ?lst, 
            "compact_stat" => ?cst, 
            "speed(KiB/s)" => speed, 
            "total_take" => ?total_take, 
            "global_load_meta_stat" => ?self.stats.load_meta_stat);
        futures::future::ok(())
    }

    fn after_execution_finished<'a>(
        &'a mut self,
        _cx: AfterFinishCtx<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        info!("All compactions done.");
        futures::future::ok(())
    }

    fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) {
        tracing_active_tree::init();

        let sigusr1_handler = async {
            let mut signal = tokio::signal::unix::signal(SignalKind::user_defined1()).unwrap();
            while let Some(_) = signal.recv().await {
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
        self.meta_len = cx.est_meta_size;
    }
}

#[derive(Default)]
struct CollectStatistic {
    load_stat: LoadStatistic,
    compact_stat: CompactStatistic,
    load_meta_stat: LoadMetaStatistic,
    collect_stat: CollectCompactionStatistic,
}

impl CollectStatistic {
    fn update_subcompaction(&mut self, res: &SubcompactionResult) {
        self.load_stat += res.load_stat.clone();
        self.compact_stat += res.compact_stat.clone();
    }

    fn update_collect_compaction_stat(&mut self, stat: &CollectCompactionStatistic) {
        self.collect_stat += stat.clone()
    }

    fn update_load_meta_stat(&mut self, stat: &LoadMetaStatistic) {
        self.load_meta_stat += stat.clone()
    }
}

#[derive(Default)]
pub struct SaveMeta {
    collector: CompactionRunInfoBuilder,
    stats: CollectStatistic,
    begin: Option<chrono::DateTime<Local>>,
}

impl SaveMeta {
    fn comments(&self) -> String {
        let now = Local::now();
        let mut comments = String::new();
        comments += &format!(
            "start_time: {}\n",
            self.begin
                .map(|v| v.to_rfc3339())
                .as_deref()
                .unwrap_or("unknown")
        );
        comments += &format!("end_time: {}\n", now.to_rfc3339());
        comments += &format!(
            "taken: {}\n",
            self.begin.map(|v| now - v).unwrap_or(Duration::zero())
        );
        comments += &format!("exec_by: {:?}\n", tikv_util::sys::hostname());
        comments += &format!("load_stat: {:?}\n", self.stats.load_stat);
        comments += &format!("compact_stat: {:?}\n", self.stats.compact_stat);
        comments += &format!("load_meta_stat: {:?}\n", self.stats.load_meta_stat);
        comments += &format!("collect_stat: {:?}\n", self.stats.collect_stat);
        comments
    }
}

impl ExecHooks for SaveMeta {
    fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) {
        self.begin = Some(Local::now());
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
            .set_artifactes(format!("{}/{}", cx.this.out_prefix, META_OUT_REL));
        run_info
            .mut_meta()
            .set_generated_files(format!("{}/{}", cx.this.out_prefix, SST_OUT_REL));
    }

    fn before_a_subcompaction_start(&mut self, _cid: CId, c: SubcompactionStartCtx<'_>) {
        self.stats
            .update_collect_compaction_stat(c.collect_compaction_stat_diff);
        self.stats.update_load_meta_stat(c.load_stat_diff);
    }

    fn after_a_subcompaction_end<'a>(
        &'a mut self,
        _cid: CId,
        cx: SubcompactionFinishCtx<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        self.collector.add_subcompaction(&cx.result);
        self.stats.update_subcompaction(&cx.result);

        async {
            use protobuf::Message;
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
    }

    fn after_execution_finished<'a>(
        &'a mut self,
        cx: AfterFinishCtx<'a>,
    ) -> impl Future<Output = Result<()>> + 'a {
        let comments = self.comments();
        self.collector.mut_meta().set_comments(comments);
        self.collector.write_migration(cx.external_storage)
    }
}
