use std::{path::Path, sync::Arc, time::Instant};

// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use chrono::Local;
pub use engine_traits::SstCompressionType;
use external_storage::{ExternalStorage, UnpinReader};
use futures::{future::TryFutureExt, io::AsyncReadExt, io::Cursor, stream::TryStreamExt};
use kvproto::brpb;
use protobuf::{Message, parse_from_bytes};
use tikv_util::{
    info,
    stream::{JustRetry, retry},
    warn,
};

use super::CollectStatistic;
use crate::{
    compaction::{META_OUT_REL, SST_OUT_REL, meta::CompactionRunInfoBuilder},
    errors::Result,
    execute::hooking::{
        AfterFinishCtx, BeforeStartCtx, CId, ExecHooks, SkipReason, SubcompactionFinishCtx,
        SubcompactionSkippedCtx, SubcompactionStartCtx,
    },
    statistic::CompactLogBackupStatistic,
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
    meta_writer: Option<MetaBatchWriter>,
}

impl Default for SaveMeta {
    fn default() -> Self {
        Self {
            collector: Default::default(),
            stats: Default::default(),
            begin: Local::now(),
            meta_writer: None,
        }
    }
}

/// A rolling batch writer for `.cmeta` objects.
///
/// It reduces object-count by writing batched `.cmeta` payloads, while still
/// persisting every finished subcompaction by overwriting the current batch
/// object.
///
/// Note: overwriting the same object per subcompaction trades write-amplification
/// for stable resume semantics and low object-count.
struct MetaBatchWriter {
    dir: String,
    current_seq: u64,
    buffer: brpb::LogFileSubcompactions,
    max_subcompactions_per_cmeta: usize,
    target_bytes_per_cmeta: usize,
}

impl MetaBatchWriter {
    const DEFAULT_MAX_SUBCOMPACTIONS_PER_CMETA: usize = 128;
    const DEFAULT_TARGET_BYTES_PER_CMETA: usize = 4 * 1024 * 1024;

    fn new(dir: String, current_seq: u64, buffer: brpb::LogFileSubcompactions) -> Self {
        Self {
            dir,
            current_seq,
            buffer,
            max_subcompactions_per_cmeta: Self::DEFAULT_MAX_SUBCOMPACTIONS_PER_CMETA,
            target_bytes_per_cmeta: Self::DEFAULT_TARGET_BYTES_PER_CMETA,
        }
    }

    fn current_key(&self) -> String {
        format!("{}/batch_{:06}.cmeta", self.dir, self.current_seq)
    }

    fn parse_batch_seq(key: &str) -> Option<u64> {
        let file_name = Path::new(key).file_name()?.to_str()?;
        let seq = file_name.strip_prefix("batch_")?;
        let seq = seq.strip_suffix(".cmeta")?;
        seq.parse().ok()
    }

    async fn load_or_new(storage: &dyn ExternalStorage, out_prefix: &str) -> Result<Self> {
        let dir = format!("{}/{}", out_prefix, META_OUT_REL);
        let list_prefix = format!("{}/", dir);

        let mut max_seq_and_key: Option<(u64, String)> = None;
        let mut stream = storage.iter_prefix(&list_prefix);
        while let Some(item) = stream.try_next().await? {
            let Some(seq) = Self::parse_batch_seq(&item.key) else {
                continue;
            };
            match &mut max_seq_and_key {
                None => max_seq_and_key = Some((seq, item.key)),
                Some((max_seq, key)) if seq > *max_seq => {
                    *max_seq = seq;
                    *key = item.key;
                }
                _ => {}
            }
        }

        let Some((max_seq, key)) = max_seq_and_key else {
            return Ok(Self::new(dir, 0, brpb::LogFileSubcompactions::new()));
        };

        let mut content = vec![];
        if let Err(err) = storage.read(&key).read_to_end(&mut content).await {
            warn!(
                "SaveMeta: failed to read existing cmeta batch, starting a new batch.";
                "key" => %key,
                "err" => %err
            );
            return Ok(Self::new(dir, max_seq + 1, brpb::LogFileSubcompactions::new()));
        }

        match parse_from_bytes::<brpb::LogFileSubcompactions>(&content) {
            Ok(buffer) => {
                let mut this = Self::new(dir, max_seq, buffer);
                let current_bytes = this.buffer.write_to_bytes()?.len();
                if this.buffer.subcompactions.len() >= this.max_subcompactions_per_cmeta
                    || current_bytes >= this.target_bytes_per_cmeta
                {
                    this.current_seq = max_seq + 1;
                    this.buffer = brpb::LogFileSubcompactions::new();
                }
                Ok(this)
            }
            Err(err) => {
                warn!(
                    "SaveMeta: failed to parse existing cmeta batch, starting a new batch.";
                    "key" => %key,
                    "err" => %err
                );
                Ok(Self::new(dir, max_seq + 1, brpb::LogFileSubcompactions::new()))
            }
        }
    }

    async fn append_and_flush(
        &mut self,
        storage: &dyn ExternalStorage,
        subcompaction: brpb::LogFileSubcompaction,
    ) -> Result<()> {
        self.buffer.mut_subcompactions().push(subcompaction);
        let bytes = self.buffer.write_to_bytes()?;
        let key = self.current_key();

        retry(|| async {
            let reader = UnpinReader(Box::new(Cursor::new(&bytes)));
            storage
                .write(&key, reader, bytes.len() as _)
                .map_err(JustRetry)
                .await
        })
        .await
        .map_err(|err| err.0)?;

        if self.buffer.subcompactions.len() >= self.max_subcompactions_per_cmeta
            || bytes.len() >= self.target_bytes_per_cmeta
        {
            self.current_seq += 1;
            self.buffer = brpb::LogFileSubcompactions::new();
        }
        Ok(())
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
        self.meta_writer = Some(MetaBatchWriter::load_or_new(cx.storage, &cx.this.out_prefix).await?);
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

    async fn on_subcompaction_skipped(&mut self, cx: SubcompactionSkippedCtx<'_>) {
        if cx.reason == SkipReason::AlreadyDone {
            self.collector.add_origin_subcompaction(cx.subc);
        }
    }

    async fn after_a_subcompaction_end(
        &mut self,
        _cid: CId,
        cx: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        self.collector.add_subcompaction(cx.result);
        self.stats.update_subcompaction(cx.result);

        let Some(writer) = self.meta_writer.as_mut() else {
            return Err(crate::ErrorKind::Other("SaveMeta: meta writer not initialized".to_owned()).into());
        };
        writer
            .append_and_flush(cx.external_storage, cx.result.meta.clone())
            .await?;
        Result::Ok(())
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        if self.collector.is_empty() {
            warn!("Nothing to write, skipping saving meta.");
            return Ok(());
        }
        let comments = self.comments();
        self.collector.mut_meta().set_comments(comments);
        let begin = Instant::now();
        self.collector
            .write_migration(Arc::clone(cx.storage))
            .await?;
        info!("Migration written."; "duration" => ?begin.elapsed());
        Ok(())
    }
}

