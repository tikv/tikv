// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
pub mod hooking;

#[cfg(test)]
mod test;

use std::{borrow::Cow, cell::Cell, path::Path, str::FromStr, sync::Arc};

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
use tikv_util::config::ReadableSize;
use tokio::{
    runtime::Handle,
    task::{JoinError, JoinHandle},
};
use tracing::trace_span;
use tracing_active_tree::{frame, root};
use txn_types::TimeStamp;

use self::hooking::AbortedCtx;
use super::{
    compaction::{
        collector::{CollectSubcompaction, CollectSubcompactionConfig},
        exec::{SubcompactExt, SubcompactionExec},
    },
    storage::{LoadFromExt, StreamMetaStorage},
};
use crate::{
    ErrorKind,
    compaction::{SubcompactionResult, exec::SubcompactionExecArg},
    errors::{Result, TraceResultExt},
    execute::hooking::SubcompactionSkippedCtx,
    util,
};

const COMPACTION_V1_PREFIX: &str = "v1/compactions";

/// Sharding configuration for `compact-log-backup`.
///
/// Sharding is defined as: `hash(token) % total == index - 1`, where `index`
/// is 1-based.
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

    /// Membership test for an input item, using `store_id` when present and
    /// falling back to hashing a stable path string.
    pub fn contains(&self, store_id: Option<u64>, fallback_path: &str) -> bool {
        match store_id {
            Some(id) if id > 0 => self.contains_store_id(id),
            _ => self.shard_mod(Self::hash64(fallback_path.as_bytes())),
        }
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

/// A CLI-facing wrapper for parsing `ShardConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardConfigArg(pub ShardConfig);

impl ShardConfigArg {
    pub fn into_inner(self) -> ShardConfig {
        self.0
    }
}

impl From<ShardConfigArg> for ShardConfig {
    fn from(value: ShardConfigArg) -> Self {
        value.0
    }
}

impl FromStr for ShardConfigArg {
    type Err = ShardConfigParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        parse_shard_config(s).map(Self)
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
    /// Filter out files doesn't contain any record with TS great or equal than
    /// this.
    pub from_ts: u64,
    /// Filter out files doesn't contain any record with TS less than this.
    pub until_ts: u64,
    /// The max count of running prefetch tasks.
    pub prefetch_running_count: u64,
    /// The max count of saved prefetch tasks in the queue.
    pub prefetch_buffer_count: u64,
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
        serializer.emit_u64("from_ts", self.from_ts)?;
        serializer.emit_u64("until_ts", self.until_ts)?;
        if let Some(shard) = self.shard {
            serializer.emit_u64("shard.index", shard.index)?;
            serializer.emit_u64("shard.total", shard.total)?;
        }
        let date = |pts| {
            let ts = TimeStamp::new(pts).physical();
            chrono::DateTime::<Utc>::from_utc(
                chrono::NaiveDateTime::from_timestamp(
                    ts as i64 / 1000,
                    (ts % 1000) as u32 * 1_000_000,
                ),
                Utc,
            )
        };
        serializer.emit_arguments("from_date", &format_args!("{}", date(self.from_ts)))?;
        serializer.emit_arguments("until_date", &format_args!("{}", date(self.until_ts)))?;
        serializer.emit_arguments("compression", &format_args!("{:?}", self.compression))?;
        if let Some(level) = self.compression_level {
            serializer.emit_i32("compression.level", level)?;
        }

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

impl Execution {
    async fn abort_and_drain<T>(pending: &mut Vec<JoinHandle<T>>) {
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

    async fn run_prepared(&self, cx: &mut ExecuteCtx<'_, impl ExecHooks>) -> Result<()> {
        let mut ext = LoadFromExt::default();
        ext.prefetch_running_count = self.cfg.prefetch_running_count as usize;
        ext.prefetch_buffer_count = self.cfg.prefetch_buffer_count as usize;

        let ExecuteCtx {
            ref storage,
            ref mut hooks,
            ..
        } = cx;

        let cx = BeforeStartCtx {
            storage: storage.as_ref(),
            async_rt: &tokio::runtime::Handle::current(),
            this: self,
        };
        hooks.before_execution_started(cx).await?;

        // Avoid setting an explicit parent here: this span may be dropped while
        // the parent span is not currently entered (e.g. early-abort paths),
        // which can violate invariants of `tracing-active-tree`.
        ext.loading_content_span = Some(trace_span!("load_meta_file_names"));

        let storage = Arc::clone(storage);
        let meta = StreamMetaStorage::load_from_ext(&storage, ext).await?;
        let shard = self.cfg.shard;
        let stream = meta.flat_map(move |file| match file {
            Ok(file) => {
                let meta_name = Arc::clone(&file.name);
                let store_id = file.store_id;
                let logs = file
                    .into_logs()
                    .filter(move |_| shard.map_or(true, |s| s.contains(store_id, &meta_name)));
                stream::iter(logs).map(Ok).left_stream()
            }
            Err(err) => stream::once(futures::future::err(err)).right_stream(),
        });
        let mut compact_stream = CollectSubcompaction::new(
            stream,
            CollectSubcompactionConfig {
                compact_from_ts: self.cfg.from_ts,
                compact_to_ts: self.cfg.until_ts,
                subcompaction_size_threshold: ReadableSize::mb(128).0,
            },
        );
        let mut pending = Vec::new();
        let mut id = 0;
        let mut first_err: Option<crate::Error> = None;

        while let Some(c) = compact_stream.next().await {
            let cstat = compact_stream.take_statistic();
            let lstat = compact_stream.get_mut().get_mut().take_statistic();

            let c = match c {
                Ok(c) => c,
                Err(err) => {
                    first_err = Some(err);
                    break;
                }
            };
            let cid = CId(id);
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
                continue;
            }

            id += 1;

            let compact_args = SubcompactionExecArg {
                out_prefix: Some(Path::new(&self.out_prefix).to_owned()),
                db: self.db.clone(),
                storage: Arc::clone(&storage),
            };
            let compact_worker = SubcompactionExec::from(compact_args);
            let mut ext = SubcompactExt::default();
            ext.max_load_concurrency = 32;
            ext.compression = self.cfg.compression;
            ext.compression_level = self.cfg.compression_level;

            let compact_work = async move {
                let res = compact_worker.run(c, ext).await.trace_err()?;
                res.verify_checksum()
                    .annotate(format_args!("the compaction is {:?}", res.origin))?;
                Result::Ok((res, cid))
            };
            let join_handle = tokio::spawn(root!(compact_work));
            pending.push(join_handle);

            if pending.len() >= self.max_concurrent_subcompaction as _ {
                let join = util::select_vec(&mut pending);
                let (cres, cid) =
                    match Self::unpack_compaction_join(frame!("wait_for_compaction"; join).await) {
                        Ok(v) => v,
                        Err(err) => {
                            first_err = Some(err);
                            break;
                        }
                    };
                if let Err(err) = self
                    .on_compaction_finish(cid, &cres, storage.as_ref(), *hooks)
                    .await
                {
                    first_err = Some(err);
                    break;
                }
            }
        }
        // Close spans created while loading metadata as early as possible.
        drop(compact_stream);

        if let Some(err) = first_err {
            Self::abort_and_drain(&mut pending).await;
            return Err(err);
        }

        while let Some(join) = pending.pop() {
            let (cres, cid) = match Self::unpack_compaction_join(frame!("final_wait"; join).await) {
                Ok(v) => v,
                Err(err) => {
                    Self::abort_and_drain(&mut pending).await;
                    return Err(err);
                }
            };
            if let Err(err) = self
                .on_compaction_finish(cid, &cres, storage.as_ref(), *hooks)
                .await
            {
                Self::abort_and_drain(&mut pending).await;
                return Err(err);
            }
        }
        let cx = AfterFinishCtx {
            async_rt: &Handle::current(),
            storage: &storage,
        };
        hooks.after_execution_finished(cx).await?;

        Result::Ok(())
    }

    pub fn run(self, mut hooks: impl ExecHooks) -> Result<()> {
        let storage =
            external_storage::create_storage(&self.external_storage, BackendConfig::default())?;
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
