// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;

pub use engine_traits::SstCompressionType;
use external_storage::ExternalStorage;
use tokio::runtime::Handle;

use crate::{
    compaction::{Subcompaction, SubcompactionResult},
    errors::Result,
    execute::Execution,
    statistic::{CollectSubcompactionStatistic, LoadMetaStatistic},
    Error,
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
    /// Whether to skip this compaction.
    pub(super) skip: &'a Cell<bool>,
}

impl<'a> SubcompactionStartCtx<'a> {
    pub fn skip(&self) {
        self.skip.set(true);
    }
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

    /// This hook will be called once the compaction failed due to some reason.
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

impl<T: ExecHooks> ExecHooks for Option<T> {
    fn before_a_subcompaction_start(&mut self, cid: CId, c: SubcompactionStartCtx<'_>) {
        if let Some(h) = self {
            h.before_a_subcompaction_start(cid, c);
        }
    }

    async fn after_a_subcompaction_end(
        &mut self,
        cid: CId,
        cx: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        if let Some(h) = self {
            h.after_a_subcompaction_end(cid, cx).await
        } else {
            Ok(())
        }
    }

    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        if let Some(h) = self {
            h.before_execution_started(cx).await
        } else {
            Ok(())
        }
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        if let Some(h) = self {
            h.after_execution_finished(cx).await
        } else {
            Ok(())
        }
    }

    async fn on_aborted(&mut self, cx: AbortedCtx<'_>) {
        if let Some(h) = self {
            h.on_aborted(cx).await
        }
    }
}
