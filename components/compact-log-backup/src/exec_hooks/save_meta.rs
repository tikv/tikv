// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use chrono::Local;
pub use engine_traits::SstCompressionType;
use external_storage::UnpinReader;
use futures::{future::TryFutureExt, io::Cursor};
use kvproto::brpb;
use tikv_util::{
    stream::{retry, JustRetry},
    warn,
};

use super::CollectStatistic;
use crate::{
    compaction::{meta::CompactionRunInfoBuilder, META_OUT_REL, SST_OUT_REL},
    errors::Result,
    execute::hooking::{
        AfterFinishCtx, BeforeStartCtx, CId, ExecHooks, SubcompactionFinishCtx,
        SubcompactionStartCtx,
    },
    statistic::CompactLogBackupStatistic,
    util,
};

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

        let first_version = cx
            .result
            .meta
            .region_meta_hints
            .first()
            .map(|h| {
                format!(
                    "_{}_{}",
                    h.get_region_epoch().get_version(),
                    h.get_region_epoch().get_conf_ver()
                )
            })
            .unwrap_or_default();
        let meta_name = format!(
            "{}_{}_{}_{}{}.cmeta",
            util::aligned_u64(cx.result.origin.input_min_ts),
            util::aligned_u64(cx.result.origin.input_max_ts),
            util::aligned_u64(cx.result.origin.crc64()),
            cx.result.origin.region_id,
            first_version
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
        if self.collector.is_empty() {
            warn!("Nothing to write, skipping saving meta.");
            return Ok(());
        }
        let comments = self.comments();
        self.collector.mut_meta().set_comments(comments);
        self.collector.write_migration(cx.storage).await
    }
}
