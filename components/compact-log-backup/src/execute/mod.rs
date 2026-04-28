// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
pub mod hooking;

#[cfg(test)]
mod test;

use std::{borrow::Cow, cell::Cell, collections::HashMap, path::Path, str::FromStr, sync::Arc};

use chrono::Utc;
use engine_rocks::RocksEngine;
pub use engine_traits::SstCompressionType;
use engine_traits::SstExt;
use external_storage::{BackendConfig, ExternalStorage};
use futures::stream::{self, StreamExt};
use hooking::{
    AfterFinishCtx, BeforeStartCtx, CId, ExecHooks, SubcompactionFinishCtx, SubcompactionStartCtx,
};
use kvproto::brpb::StorageBackend;
use slog_global::info;
use tikv_util::config::ReadableSize;
use tokio::{runtime::Handle, task::JoinError};
use tokio_stream::Stream;
use tracing::trace_span;
use tracing_active_tree::{frame, root};

use self::hooking::AbortedCtx;
use super::{
    compaction::{
        Subcompaction,
        collector::{CollectCachedSubcompaction, CollectSubcompaction, CollectSubcompactionConfig},
        exec::{SubcompactExt, SubcompactionExec},
    },
    statistic::{CollectSubcompactionStatistic, LoadMetaStatistic},
    storage::{CountObjectsExt, LoadFromExt, LogFile, StreamMetaStorage},
};
use crate::{
    ErrorKind,
    compaction::{SubcompactionResult, exec::SubcompactionExecArg},
    errors::{Result, TraceResultExt},
    execute::hooking::SubcompactionSkippedCtx,
    source::PhysicalFileCache,
    util,
};

const COMPACTION_V1_PREFIX: &str = "v1/compactions";
const CACHED_COMPACTION_BATCH_TARGET_BYTES: u64 = ReadableSize::mb(512).0;
const CACHED_COMPACTION_BATCH_MAX_COUNT: usize = 64;

/// Sharding configuration for `compact-log-backup`.
///
/// Sharding is defined as: `hash(store_id.to_le_bytes()) % total == index - 1`,
/// where `index` is 1-based.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardConfig {
    /// Shard index (1-based).
    pub index: u64,
    /// Total shards (must be > 0).
    pub total: u64,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ShardConfigError {
    #[error("TOTAL must be > 0")]
    TotalIsZero,
    #[error("INDEX must be within [1, {total}]")]
    IndexOutOfRange { index: u64, total: u64 },
}

impl ShardConfig {
    pub fn new(index: u64, total: u64) -> std::result::Result<Self, ShardConfigError> {
        if total == 0 {
            return Err(ShardConfigError::TotalIsZero);
        }
        if index == 0 || index > total {
            return Err(ShardConfigError::IndexOutOfRange { index, total });
        }
        Ok(Self { index, total })
    }

    fn shard_mod(&self, v: u64) -> bool {
        (v % self.total) == (self.index - 1)
    }

    fn hash64(bytes: &[u8]) -> u64 {
        let mut hasher = crc64fast::Digest::new();
        hasher.write(bytes);
        hasher.sum64()
    }

    pub fn contains_store_id(&self, store_id: u64) -> bool {
        self.shard_mod(Self::hash64(&store_id.to_le_bytes()))
    }

    pub fn suffix(&self) -> String {
        format!("shard{}_of{}", self.index, self.total)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShardConfigParseError {
    #[error("expected format INDEX/TOTAL (e.g. 1/3)")]
    InvalidFormat,
    #[error("cannot parse INDEX as u64: {0}")]
    InvalidIndex(#[source] std::num::ParseIntError),
    #[error("cannot parse TOTAL as u64: {0}")]
    InvalidTotal(#[source] std::num::ParseIntError),
    #[error(transparent)]
    InvalidShardConfig(#[from] ShardConfigError),
}

pub fn parse_shard_config(s: &str) -> std::result::Result<ShardConfig, ShardConfigParseError> {
    let (index, total) = s
        .split_once('/')
        .ok_or(ShardConfigParseError::InvalidFormat)?;
    let index = index
        .parse::<u64>()
        .map_err(ShardConfigParseError::InvalidIndex)?;
    let total = total
        .parse::<u64>()
        .map_err(ShardConfigParseError::InvalidTotal)?;
    Ok(ShardConfig::new(index, total)?)
}

impl FromStr for ShardConfig {
    type Err = ShardConfigParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        parse_shard_config(s)
    }
}

/// The config for an execution of a compaction.
///
/// This structure itself fully defines what work the compaction need to do.
/// That is, keeping this structure unchanged, the compaction task generated
/// should be the same. (But some of them may be filtered out later.)
#[derive(Debug)]
pub struct ExecutionConfig {
    /// Optional sharding configuration. When set, only inputs belonging to
    /// this shard will be compacted.
    pub shard: Option<ShardConfig>,
    /// Lower bound for selecting default CF files. Defaults to `from_ts`.
    pub shift_ts: u64,
    /// Whether to calculate `shift_ts` from backup metadata names before
    /// collecting subcompactions.
    pub calculate_shift_ts: bool,
    /// Filter out write CF files don't have any record with TS less than this.
    pub from_ts: u64,
    /// Filter out write CF files don't have any record with TS great or equal
    /// than this.
    pub until_ts: u64,
    /// The max count of running prefetch tasks.
    pub prefetch_running_count: u64,
    /// The max count of saved prefetch tasks in the queue.
    pub prefetch_buffer_count: u64,
    /// Bytes reserved for caching raw physical log files. Zero disables the
    /// cache and keeps the historical per-logical-file range downloads.
    pub physical_file_cache_capacity: u64,
    /// The compress algorithm we are going to use for output.
    pub compression: SstCompressionType,
    /// The compress level we are going to use.
    ///
    /// If `None`, we will use the default level of the selected algorithm.
    pub compression_level: Option<i32>,
}

impl slog::KV for ExecutionConfig {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_u64("shift_ts", self.shift_ts)?;
        serializer.emit_bool("calculate_shift_ts", self.calculate_shift_ts)?;
        serializer.emit_u64("from_ts", self.from_ts)?;
        serializer.emit_u64("until_ts", self.until_ts)?;
        if let Some(shard) = self.shard {
            serializer.emit_u64("shard.index", shard.index)?;
            serializer.emit_u64("shard.total", shard.total)?;
        }
        let date = |pts| {
            chrono::DateTime::<Utc>::from_timestamp_millis(pts as i64)
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| format!("invalid_ts({pts})"))
        };
        serializer.emit_arguments("shift_date", &format_args!("{}", date(self.shift_ts)))?;
        serializer.emit_arguments("from_date", &format_args!("{}", date(self.from_ts)))?;
        serializer.emit_arguments("until_date", &format_args!("{}", date(self.until_ts)))?;
        serializer.emit_arguments("compression", &format_args!("{:?}", self.compression))?;
        if let Some(level) = self.compression_level {
            serializer.emit_i32("compression.level", level)?;
        }
        serializer.emit_u64(
            "physical_file_cache_capacity",
            self.physical_file_cache_capacity,
        )?;

        Ok(())
    }
}

impl ExecutionConfig {
    /// Create a suitable (but not forced) prefix for the artifices of the
    /// compaction.
    ///
    /// You may specify a `name`, which will be included in the path, then the
    /// compaction will be easier to be found.
    pub fn recommended_prefix(&self, name: &str) -> String {
        let mut hasher = crc64fast::Digest::new();
        hasher.write(name.as_bytes());
        if let Some(shard) = self.shard {
            hasher.write(&shard.index.to_le_bytes());
            hasher.write(&shard.total.to_le_bytes());
        }
        hasher.write(&self.from_ts.to_le_bytes());
        hasher.write(&self.until_ts.to_le_bytes());
        hasher.write(&[self.calculate_shift_ts as u8]);
        hasher.write(&util::compression_type_to_u8(self.compression).to_le_bytes());
        hasher.write(&self.compression_level.unwrap_or(0).to_le_bytes());

        let shard_suffix = self
            .shard
            .map(|s| format!("_{}", s.suffix()))
            .unwrap_or_default();
        format!(
            "{}/{}_{}{}",
            COMPACTION_V1_PREFIX,
            name,
            util::aligned_u64(hasher.sum64()),
            shard_suffix
        )
    }
}

/// An execution of compaction.
pub struct Execution<DB: SstExt = RocksEngine> {
    /// The configuration.
    pub cfg: ExecutionConfig,

    /// Max subcompactions can be executed concurrently.
    pub max_concurrent_subcompaction: u64,
    /// The external storage for input and output.
    pub external_storage: StorageBackend,
    /// Backend configuration for constructing external storage.
    pub backend_config: BackendConfig,
    /// The RocksDB instance for creating `SstWriter`.
    /// By design little or no data will be written to the instance, for now
    /// this is only used for loading the user collected properties
    /// configuration.
    pub db: Option<DB>,
    /// The prefix of the artifices.
    pub out_prefix: String,
}

struct ExecuteCtx<'a, H: ExecHooks> {
    storage: &'a Arc<dyn ExternalStorage + 'static>,
    hooks: &'a mut H,
}

type FinishedCompaction = Result<(SubcompactionResult, CId)>;
type FinishedCompactions = Vec<FinishedCompaction>;
type CompactJoin = tokio::task::JoinHandle<Result<FinishedCompactions>>;

trait TakeLoadMetaStatistic {
    fn take_load_meta_statistic(&mut self) -> LoadMetaStatistic;
}

impl TakeLoadMetaStatistic for StreamMetaStorage<'_> {
    fn take_load_meta_statistic(&mut self) -> LoadMetaStatistic {
        self.take_statistic()
    }
}

impl<St, U, F> TakeLoadMetaStatistic for futures::stream::FlatMap<St, U, F>
where
    St: TakeLoadMetaStatistic,
{
    fn take_load_meta_statistic(&mut self) -> LoadMetaStatistic {
        self.get_mut().take_load_meta_statistic()
    }
}

fn cached_compaction_batches(
    runnable: Vec<(Subcompaction, CId)>,
) -> impl Iterator<Item = Vec<(Subcompaction, CId)>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 0;

    for (c, cid) in runnable {
        if !batch.is_empty()
            && (batch.len() >= CACHED_COMPACTION_BATCH_MAX_COUNT
                || batch_bytes + c.size > CACHED_COMPACTION_BATCH_TARGET_BYTES)
        {
            batches.push(std::mem::take(&mut batch));
            batch_bytes = 0;
        }
        batch_bytes += c.size;
        batch.push((c, cid));
    }
    if !batch.is_empty() {
        batches.push(batch);
    }

    batches.into_iter()
}

impl Execution {
    async fn abort_and_drain<T>(pending: &mut Vec<tokio::task::JoinHandle<T>>) {
        for join in pending.iter() {
            join.abort();
        }
        while let Some(join) = pending.pop() {
            let _ = join.await;
        }
    }

    fn unpack_compaction_join<T>(join_res: std::result::Result<Result<T>, JoinError>) -> Result<T> {
        match join_res {
            Ok(res) => res,
            Err(join_err) => {
                Err(ErrorKind::Other(format!("subcompaction task join error: {join_err}")).into())
            }
        }
    }

    pub fn gen_name(&self) -> String {
        let compaction_name = Path::new(&self.out_prefix)
            .file_name()
            .map(|v| v.to_string_lossy())
            .unwrap_or(Cow::Borrowed("unknown"));
        let pid = tikv_util::sys::thread::thread_id();
        let hostname = tikv_util::sys::hostname();
        format!(
            "{}#{}@{}",
            compaction_name,
            pid,
            hostname.as_deref().unwrap_or("unknown")
        )
    }

    fn subcompact_ext(&self) -> SubcompactExt {
        let mut ext = SubcompactExt::default();
        ext.max_load_concurrency = 32;
        ext.compression = self.cfg.compression;
        ext.compression_level = self.cfg.compression_level;
        ext
    }

    fn spawn_subcompaction(
        &self,
        storage: &Arc<dyn ExternalStorage>,
        c: Subcompaction,
        cid: CId,
        physical_file_cache: Option<Arc<PhysicalFileCache>>,
    ) -> CompactJoin {
        let compact_args = SubcompactionExecArg {
            out_prefix: Some(Path::new(&self.out_prefix).to_owned()),
            db: self.db.clone(),
            storage: Arc::clone(storage),
            physical_file_cache,
        };
        let compact_worker = SubcompactionExec::from(compact_args);
        let ext = self.subcompact_ext();

        let compact_work = async move {
            let res = async {
                let res = compact_worker.run(c, ext).await.trace_err()?;
                res.verify_checksum()
                    .annotate(format_args!("the compaction is {:?}", res.origin))?;
                Result::Ok((res, cid))
            }
            .await;
            Result::Ok(vec![res])
        };
        tokio::spawn(root!(compact_work))
    }

    fn spawn_subcompaction_batch(
        &self,
        storage: &Arc<dyn ExternalStorage>,
        batch: Vec<(Subcompaction, CId)>,
        physical_file_cache: Arc<PhysicalFileCache>,
    ) -> CompactJoin {
        let out_prefix = Some(Path::new(&self.out_prefix).to_owned());
        let db = self.db.clone();
        let storage = Arc::clone(storage);
        let ext = self.subcompact_ext();

        let compact_work = async move {
            let mut results = Vec::with_capacity(batch.len());
            for (c, cid) in batch {
                let compact_args = SubcompactionExecArg {
                    out_prefix: out_prefix.clone(),
                    db: db.clone(),
                    storage: Arc::clone(&storage),
                    physical_file_cache: Some(Arc::clone(&physical_file_cache)),
                };
                let compact_worker = SubcompactionExec::from(compact_args);
                let res = async {
                    let res = compact_worker.run(c, ext).await.trace_err()?;
                    res.verify_checksum()
                        .annotate(format_args!("the compaction is {:?}", res.origin))?;
                    Result::Ok((res, cid))
                }
                .await;
                let failed = res.is_err();
                results.push(res);
                if failed {
                    break;
                }
            }
            Result::Ok(results)
        };
        tokio::spawn(root!(compact_work))
    }

    async fn wait_one_compaction(
        &self,
        pending: &mut Vec<CompactJoin>,
        storage: &dyn ExternalStorage,
        hooks: &mut impl ExecHooks,
    ) -> Result<()> {
        let join = util::select_vec(pending);
        let finished = Self::unpack_compaction_join(frame!("wait_for_compaction"; join).await)?;
        for item in finished {
            let (cres, cid) = item?;
            self.on_compaction_finish(cid, &cres, storage, hooks)
                .await?;
        }
        Ok(())
    }

    async fn drain_compactions(
        &self,
        pending: &mut Vec<CompactJoin>,
        storage: &dyn ExternalStorage,
        hooks: &mut impl ExecHooks,
    ) -> Result<()> {
        while let Some(join) = pending.pop() {
            let finished = Self::unpack_compaction_join(frame!("final_wait"; join).await)?;
            for item in finished {
                let (cres, cid) = item?;
                self.on_compaction_finish(cid, &cres, storage, hooks)
                    .await?;
            }
        }
        Ok(())
    }

    async fn push_subcompaction(
        &self,
        pending: &mut Vec<CompactJoin>,
        storage: &Arc<dyn ExternalStorage>,
        hooks: &mut impl ExecHooks,
        c: Subcompaction,
        cid: CId,
        physical_file_cache: Option<Arc<PhysicalFileCache>>,
    ) -> Result<()> {
        let join = self.spawn_subcompaction(storage, c, cid, physical_file_cache);
        pending.push(join);
        if pending.len() >= self.max_concurrent_subcompaction as _ {
            self.wait_one_compaction(pending, storage.as_ref(), hooks)
                .await?;
        }
        Ok(())
    }

    async fn push_subcompaction_batch(
        &self,
        pending: &mut Vec<CompactJoin>,
        storage: &Arc<dyn ExternalStorage>,
        hooks: &mut impl ExecHooks,
        batch: Vec<(Subcompaction, CId)>,
        physical_file_cache: Arc<PhysicalFileCache>,
    ) -> Result<()> {
        let batch_len = batch.len();
        let batch_bytes = batch.iter().map(|(c, _)| c.size).sum::<u64>();
        info!(
            "spawning cached compaction batch";
            "subcompactions" => batch_len,
            "logical_bytes" => batch_bytes,
        );
        let join = self.spawn_subcompaction_batch(storage, batch, physical_file_cache);
        pending.push(join);
        if pending.len() >= self.max_concurrent_subcompaction as _ {
            self.wait_one_compaction(pending, storage.as_ref(), hooks)
                .await?;
        }
        Ok(())
    }

    async fn prepare_subcompaction(
        &self,
        next_id: &mut u64,
        hooks: &mut impl ExecHooks,
        c: Subcompaction,
        cstat: CollectSubcompactionStatistic,
        lstat: LoadMetaStatistic,
    ) -> Option<(Subcompaction, CId)> {
        let cid = CId(*next_id);
        let skip = Cell::new(None);
        let cx = SubcompactionStartCtx {
            subc: &c,
            load_stat_diff: &lstat,
            collect_compaction_stat_diff: &cstat,
            skip: &skip,
        };
        hooks.before_a_subcompaction_start(cid, cx);
        if let Some(reason) = skip.get() {
            let skipped_cx = SubcompactionSkippedCtx { subc: &c, reason };
            hooks.on_subcompaction_skipped(skipped_cx).await;
            return None;
        }
        *next_id += 1;
        Some((c, cid))
    }

    async fn run_streaming_subcompactions<S>(
        &self,
        compact_stream: &mut CollectSubcompaction<S>,
        storage: &Arc<dyn ExternalStorage>,
        hooks: &mut impl ExecHooks,
        pending: &mut Vec<CompactJoin>,
    ) -> Result<()>
    where
        S: Stream<Item = Result<LogFile>> + TakeLoadMetaStatistic + Unpin,
    {
        let mut id = 0;
        while let Some(c) = compact_stream.next().await {
            let cstat = compact_stream.take_statistic();
            let lstat = compact_stream.get_mut().take_load_meta_statistic();
            let c = c?;
            if let Some((c, cid)) = self
                .prepare_subcompaction(&mut id, hooks, c, cstat, lstat)
                .await
            {
                self.push_subcompaction(pending, storage, hooks, c, cid, None)
                    .await?;
            }
        }
        Ok(())
    }

    async fn run_cached_subcompactions<S>(
        &self,
        compact_stream: &mut CollectCachedSubcompaction<S>,
        storage: &Arc<dyn ExternalStorage>,
        hooks: &mut impl ExecHooks,
        pending: &mut Vec<CompactJoin>,
        physical_file_cache: Arc<PhysicalFileCache>,
    ) -> Result<()>
    where
        S: Stream<Item = Result<LogFile>> + TakeLoadMetaStatistic + Unpin,
    {
        let mut id = 0;
        while let Some(window) = compact_stream.next().await {
            let mut cstat = compact_stream.take_statistic();
            let mut lstat = compact_stream.get_mut().take_load_meta_statistic();
            let window = window?;
            let mut physical_files = HashMap::new();
            let mut logical_files = 0;
            let mut logical_bytes = 0;
            for c in &window {
                logical_files += c.inputs.len();
                logical_bytes += c.size;
                for input in &c.inputs {
                    physical_files
                        .entry(input.id.name.clone())
                        .or_insert(input.physical_file_size);
                }
            }
            let physical_bytes = physical_files.values().sum::<u64>();
            info!(
                "collected physical file cache window";
                "subcompactions" => window.len(),
                "logical_files" => logical_files,
                "logical_bytes" => logical_bytes,
                "physical_files" => physical_files.len(),
                "physical_bytes" => physical_bytes,
                "physical_file_cache_capacity" => self.cfg.physical_file_cache_capacity,
            );
            let mut runnable = Vec::new();
            for c in window {
                let cstat_diff = std::mem::take(&mut cstat);
                let lstat_diff = std::mem::take(&mut lstat);
                if let Some((c, cid)) = self
                    .prepare_subcompaction(&mut id, hooks, c, cstat_diff, lstat_diff)
                    .await
                {
                    physical_file_cache.register_inputs(&c.inputs).await;
                    runnable.push((c, cid));
                }
            }

            for batch in cached_compaction_batches(runnable) {
                self.push_subcompaction_batch(
                    pending,
                    storage,
                    hooks,
                    batch,
                    Arc::clone(&physical_file_cache),
                )
                .await?;
            }
            self.drain_compactions(pending, storage.as_ref(), hooks)
                .await?;
        }
        Ok(())
    }

    async fn run_prepared(&mut self, cx: &mut ExecuteCtx<'_, impl ExecHooks>) -> Result<()> {
        let mut ext = LoadFromExt::default();
        ext.prefetch_running_count = self.cfg.prefetch_running_count as usize;
        ext.prefetch_buffer_count = self.cfg.prefetch_buffer_count as usize;

        let ExecuteCtx {
            ref storage,
            ref mut hooks,
            ..
        } = cx;
        let metadata_scan = StreamMetaStorage::count_objects(
            storage.as_ref(),
            CountObjectsExt {
                calculate_shift_ts: self.cfg.calculate_shift_ts,
                from_ts: self.cfg.from_ts,
                until_ts: self.cfg.until_ts,
            },
        )
        .await?;
        self.cfg.shift_ts = metadata_scan.shift_ts;

        let cx = BeforeStartCtx {
            storage: storage.as_ref(),
            async_rt: &tokio::runtime::Handle::current(),
            this: self,
            meta_count: metadata_scan.count,
        };
        hooks.before_execution_started(cx).await?;

        // Avoid setting an explicit parent here: this span may be dropped while
        // the parent span is not currently entered (e.g. early-abort paths),
        // which can violate invariants of `tracing-active-tree`.
        ext.loading_content_span = Some(trace_span!("load_meta_file_names"));
        ext.shard = self.cfg.shard;

        let storage = Arc::clone(storage);
        let meta = StreamMetaStorage::load_from_ext(&storage, ext).await?;
        let stream = meta.flat_map(move |file| match file {
            Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
            Err(err) => stream::once(futures::future::err(err)).right_stream(),
        });
        let collect_cfg = CollectSubcompactionConfig {
            compact_shift_from_ts: self.cfg.shift_ts,
            compact_from_ts: self.cfg.from_ts,
            compact_to_ts: self.cfg.until_ts,
            subcompaction_size_threshold: ReadableSize::mb(128).0,
        };
        let physical_file_cache = (self.cfg.physical_file_cache_capacity > 0).then(|| {
            Arc::new(PhysicalFileCache::new(
                self.cfg.physical_file_cache_capacity,
            ))
        });
        let mut pending = Vec::new();

        let schedule_res = if let Some(physical_file_cache) = physical_file_cache {
            let mut compact_stream = CollectCachedSubcompaction::new(
                stream,
                collect_cfg,
                self.cfg.physical_file_cache_capacity,
            );
            self.run_cached_subcompactions(
                &mut compact_stream,
                &storage,
                *hooks,
                &mut pending,
                physical_file_cache,
            )
            .await
        } else {
            let mut compact_stream = CollectSubcompaction::new(stream, collect_cfg);
            self.run_streaming_subcompactions(&mut compact_stream, &storage, *hooks, &mut pending)
                .await
        };

        if let Err(err) = schedule_res {
            Self::abort_and_drain(&mut pending).await;
            return Err(err);
        }

        if let Err(err) = self
            .drain_compactions(&mut pending, storage.as_ref(), *hooks)
            .await
        {
            Self::abort_and_drain(&mut pending).await;
            return Err(err);
        }
        let cx = AfterFinishCtx {
            async_rt: &Handle::current(),
            this: self,
            storage: &storage,
        };
        hooks.after_execution_finished(cx).await?;

        Result::Ok(())
    }

    pub fn run(mut self, mut hooks: impl ExecHooks) -> Result<()> {
        let storage =
            external_storage::create_storage(&self.external_storage, self.backend_config.clone())?;
        let storage: Arc<dyn ExternalStorage> = Arc::from(storage);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut cx = ExecuteCtx {
            storage: &storage,
            hooks: &mut hooks,
        };

        let guarded = async {
            let all_works = self.run_prepared(&mut cx);
            let res = tokio::select! {
                res = all_works => res,
                _ = tokio::signal::ctrl_c() => Err(ErrorKind::Other("User canceled by Ctrl-C".to_owned()).into())
            };

            if let Err(ref err) = res {
                cx.hooks
                    .on_aborted(AbortedCtx {
                        storage: cx.storage.as_ref(),
                        err,
                    })
                    .await
            }

            res
        };

        runtime.block_on(root!(guarded))
    }

    async fn on_compaction_finish(
        &self,
        cid: CId,
        result: &SubcompactionResult,
        external_storage: &dyn ExternalStorage,
        hooks: &mut impl ExecHooks,
    ) -> Result<()> {
        let cx = SubcompactionFinishCtx {
            this: self,
            external_storage,
            result,
        };
        hooks.after_a_subcompaction_end(cid, cx).await?;
        Result::Ok(())
    }
}
