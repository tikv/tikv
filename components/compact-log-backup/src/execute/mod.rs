// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
pub mod hooking;

#[cfg(test)]
mod test;

use std::{borrow::Cow, cell::Cell, path::Path, sync::Arc};

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
use tokio::runtime::Handle;
use tracing::{trace_span, Instrument};
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
    compaction::{exec::SubcompactionExecArg, SubcompactionResult},
    errors::{Result, TraceResultExt},
    util, ErrorKind,
};

/// The config for an execution of a compaction.
///
/// This structure itself fully defines what work the compaction need to do.
/// That is, keeping this structure unchanged, the compaction should always
/// generate the same artifices.
#[derive(Debug)]
pub struct ExecutionConfig {
    /// Filter out files doesn't contain any record with TS great or equal than
    /// this.
    pub from_ts: u64,
    /// Filter out files doesn't contain any record with TS less than this.
    pub until_ts: u64,
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
        hasher.write(&self.from_ts.to_le_bytes());
        hasher.write(&self.until_ts.to_le_bytes());
        hasher.write(&util::compression_type_to_u8(self.compression).to_le_bytes());
        hasher.write(&self.compression_level.unwrap_or(0).to_le_bytes());

        format!("{}_{}", name, util::aligned_u64(hasher.sum64()))
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
    /// The RocksDB instance for generating SST.
    pub db: Option<DB>,
    /// The prefix of the artifices.
    pub out_prefix: String,
}

struct ExecuteCtx<'a, H: ExecHooks> {
    storage: &'a Arc<dyn ExternalStorage>,
    hooks: &'a mut H,
}

impl Execution {
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
        let next_compaction = trace_span!("next_compaction");
        ext.max_concurrent_fetch = 128;
        ext.loading_content_span = Some(trace_span!(
            parent: next_compaction.clone(),
            "load_meta_file_names"
        ));

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

        let meta = StreamMetaStorage::load_from_ext(storage.as_ref(), ext);
        let stream = meta.flat_map(|file| match file {
            Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
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

        while let Some(c) = compact_stream
            .next()
            .instrument(next_compaction.clone())
            .await
        {
            let cstat = compact_stream.take_statistic();
            let lstat = compact_stream.get_mut().get_mut().take_statistic();

            let c = c?;
            let cid = CId(id);
            let skip = Cell::new(false);
            let cx = SubcompactionStartCtx {
                subc: &c,
                load_stat_diff: &lstat,
                collect_compaction_stat_diff: &cstat,
                skip: &skip,
            };
            hooks.before_a_subcompaction_start(cid, cx);
            if skip.get() {
                continue;
            }

            id += 1;

            let compact_args = SubcompactionExecArg {
                out_prefix: Some(Path::new(&self.out_prefix).to_owned()),
                db: self.db.clone(),
                storage: Arc::clone(storage) as _,
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
                let (cres, cid) = frame!("wait_for_compaction"; join).await.unwrap()?;
                self.on_compaction_finish(cid, &cres, storage.as_ref(), *hooks)
                    .await?;
            }
        }
        drop(next_compaction);

        for join in pending {
            let (cres, cid) = frame!("final_wait"; join).await.unwrap()?;
            self.on_compaction_finish(cid, &cres, storage.as_ref(), *hooks)
                .await?;
        }
        let cx = AfterFinishCtx {
            async_rt: &Handle::current(),
            storage: storage.as_ref(),
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

        runtime.block_on(frame!(guarded))
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
