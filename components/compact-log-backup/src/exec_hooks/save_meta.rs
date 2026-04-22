use std::{sync::Arc, time::Instant};

// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use chrono::Local;
pub use engine_traits::SstCompressionType;
use external_storage::{ExternalStorage, UnpinReader};
use futures::{
    future::TryFutureExt,
    io::{AsyncReadExt, Cursor},
    stream::TryStreamExt,
};
use kvproto::brpb;
use protobuf::Message;
use tikv_util::{
    info,
    stream::{JustRetry, retry},
    warn,
};
use uuid::Uuid;

use super::CollectStatistic;
use crate::{
    ErrorKind,
    compaction::{Input, META_OUT_REL, SST_OUT_REL, Subcompaction, meta::CompactionRunInfoBuilder},
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
    batch_cfg: BatchConfig,
}

impl Default for SaveMeta {
    fn default() -> Self {
        Self {
            collector: Default::default(),
            stats: Default::default(),
            begin: Local::now(),
            meta_writer: None,
            batch_cfg: BatchConfig::default(),
        }
    }
}

#[derive(Clone, Copy)]
struct BatchConfig {
    max_subcompactions_per_batch: usize,
    target_bytes_per_batch: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_subcompactions_per_batch: 128,
            target_bytes_per_batch: 4 * 1024 * 1024,
        }
    }
}

struct MetaBatchWriter {
    artifacts_dir: String,
    run_id: String,
    next_batch_seq: u64,
    buffer: brpb::LogFileSubcompactions,
    cfg: BatchConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CheckpointInput {
    pub path: String,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CheckpointedSubcompaction {
    pub inputs: Vec<CheckpointInput>,
}

impl CheckpointInput {
    fn from_input(input: &Input) -> Self {
        Self {
            path: input.id.name.to_string(),
            offset: input.id.offset,
            length: input.id.length,
        }
    }
}

impl CheckpointedSubcompaction {
    pub(crate) fn new(mut inputs: Vec<CheckpointInput>) -> Self {
        inputs.sort_unstable();
        Self { inputs }
    }

    pub(crate) fn from_subcompaction(subcompaction: &Subcompaction) -> Self {
        Self::new(
            subcompaction
                .inputs
                .iter()
                .map(CheckpointInput::from_input)
                .collect(),
        )
    }

    fn from_meta(subcompaction: &brpb::LogFileSubcompaction) -> Self {
        Self::new(
            subcompaction
                .get_meta()
                .get_sources()
                .iter()
                .flat_map(|source| {
                    source.get_spans().iter().map(move |span| CheckpointInput {
                        path: source.get_path().to_owned(),
                        offset: span.offset,
                        length: span.length,
                    })
                })
                .collect(),
        )
    }
}

fn final_artifacts_prefix(out_prefix: &str) -> String {
    format!("{}/{}", out_prefix, META_OUT_REL)
}

async fn list_subcompaction_meta_keys(
    storage: &dyn ExternalStorage,
    artifacts_prefix: &str,
) -> Result<Vec<String>> {
    let mut stream = storage.iter_prefix(artifacts_prefix);
    let mut keys = Vec::new();
    while let Some(item) = stream.try_next().await? {
        if item.key.ends_with(".cmeta") {
            keys.push(item.key);
        }
    }
    keys.sort();
    Ok(keys)
}

pub(crate) async fn load_checkpointed_subcompactions(
    storage: &dyn ExternalStorage,
    artifacts_prefix: &str,
) -> Result<Vec<CheckpointedSubcompaction>> {
    let mut subcompactions = Vec::new();
    for key in list_subcompaction_meta_keys(storage, artifacts_prefix).await? {
        let mut content = vec![];
        match storage.read(&key).read_to_end(&mut content).await {
            Ok(_) => match protobuf::parse_from_bytes::<brpb::LogFileSubcompactions>(&content) {
                Ok(metas) => subcompactions.extend(
                    metas
                        .subcompactions
                        .iter()
                        .map(CheckpointedSubcompaction::from_meta),
                ),
                Err(err) => {
                    warn!("SaveMeta: failed to load subcompaction meta batch, ignoring it.";
                        "key" => %key,
                        "err" => %err);
                }
            },
            Err(err) => {
                warn!("SaveMeta: failed to read subcompaction meta batch, ignoring it.";
                    "key" => %key,
                    "err" => %err);
            }
        }
    }
    Ok(subcompactions)
}

impl MetaBatchWriter {
    fn new(out_prefix: &str, cfg: BatchConfig) -> Self {
        Self {
            artifacts_dir: final_artifacts_prefix(out_prefix),
            run_id: Uuid::new_v4().to_string(),
            next_batch_seq: 0,
            buffer: brpb::LogFileSubcompactions::new(),
            cfg,
        }
    }

    fn should_flush(&self, current_bytes: usize) -> bool {
        self.buffer.subcompactions.len() >= self.cfg.max_subcompactions_per_batch
            || current_bytes >= self.cfg.target_bytes_per_batch
    }

    async fn write_bytes(storage: &dyn ExternalStorage, key: &str, bytes: &[u8]) -> Result<()> {
        retry(|| async {
            let reader = UnpinReader(Box::new(Cursor::new(bytes)));
            storage
                .write(key, reader, bytes.len() as _)
                .map_err(JustRetry)
                .await
        })
        .await
        .map_err(|err| err.0)?;
        Ok(())
    }

    async fn append_and_flush_if_needed(
        &mut self,
        storage: &dyn ExternalStorage,
        subcompaction_meta: brpb::LogFileSubcompaction,
    ) -> Result<()> {
        self.buffer.mut_subcompactions().push(subcompaction_meta);
        let current_bytes = self.buffer.compute_size();
        if self.should_flush(current_bytes as usize) {
            self.flush(storage).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, storage: &dyn ExternalStorage) -> Result<()> {
        if self.buffer.subcompactions.is_empty() {
            return Ok(());
        }
        let bytes = self.buffer.write_to_bytes()?;
        self.flush_bytes(storage, &bytes).await
    }

    async fn flush_bytes(&mut self, storage: &dyn ExternalStorage, bytes: &[u8]) -> Result<()> {
        let batch_id = format!("{}_{}", self.run_id, self.next_batch_seq);
        let batch_file_name = format!("batch_{}.cmeta", batch_id);
        let batch_key = format!("{}/{}", self.artifacts_dir, batch_file_name);
        Self::write_bytes(storage, &batch_key, bytes).await?;

        self.next_batch_seq += 1;
        self.buffer = brpb::LogFileSubcompactions::new();
        Ok(())
    }
}

impl SaveMeta {
    #[cfg(test)]
    pub(crate) fn with_batch_limits(
        mut self,
        max_subcompactions_per_batch: usize,
        target_bytes_per_batch: usize,
    ) -> Self {
        self.batch_cfg = BatchConfig {
            max_subcompactions_per_batch: max_subcompactions_per_batch.max(1),
            target_bytes_per_batch: target_bytes_per_batch.max(1),
        };
        self
    }

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
        self.meta_writer = Some(MetaBatchWriter::new(&cx.this.out_prefix, self.batch_cfg));

        let meta = self.collector.mut_meta();
        meta.set_name(cx.this.gen_name());
        meta.set_compaction_from_ts(cx.this.cfg.from_ts);
        meta.set_compaction_until_ts(cx.this.cfg.until_ts);
        meta.set_artifacts(final_artifacts_prefix(&cx.this.out_prefix));
        meta.set_generated_files(format!("{}/{}", cx.this.out_prefix, SST_OUT_REL));
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

        let writer = self.meta_writer.as_mut().ok_or_else(|| {
            crate::Error::from(ErrorKind::Other(
                "SaveMeta: meta writer hasn't been initialized".to_owned(),
            ))
        })?;
        writer
            .append_and_flush_if_needed(cx.external_storage, cx.result.meta.clone())
            .await?;
        Result::Ok(())
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        if self.collector.is_empty() {
            warn!("Nothing to write, skipping saving meta.");
            return Ok(());
        }
        if let Some(writer) = self.meta_writer.as_mut() {
            writer.flush(cx.storage.as_ref()).await?;
        }

        let comments = self.comments();
        self.collector.mut_meta().set_comments(comments);
        let begin = Instant::now();
        self.collector
            .write_migration(Arc::clone(cx.storage), cx.this.cfg.shard)
            .await?;
        info!("Migration written."; "duration" => ?begin.elapsed());
        Ok(())
    }
}
