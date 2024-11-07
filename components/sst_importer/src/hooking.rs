// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{any::Any, marker::PhantomData, path::PathBuf, sync::Arc};

use futures_util::{future::BoxFuture, lock::OwnedMutexGuard};
use kvproto::import_sstpb::SstMeta;

use crate::errors::Result;

#[derive(Clone, Copy)]
pub struct BeforeProposeIngestCtx<'a> {
    pub sst_meta: &'a [SstMeta],
    pub sst_to_path: &'a dyn Fn(&SstMeta) -> Result<PathBuf>,
}

#[derive(Clone, Copy)]
pub struct AfterIngestedCtx<'a> {
    pub sst_meta: &'a [SstMeta],
}

pub trait ImportHook: Any {
    fn before_propose_ingest(&self, cx: BeforeProposeIngestCtx<'_>) -> BoxFuture<'_, Result<()>>;
    fn after_ingested(&self, cx: AfterIngestedCtx<'_>) -> BoxFuture<'_, ()>;
}

#[derive(Default)]
pub struct NopHooks {
    _non_clone: PhantomData<OwnedMutexGuard<()>>,
}

impl ImportHook for NopHooks {
    fn before_propose_ingest(&self, _: BeforeProposeIngestCtx<'_>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn after_ingested(&self, _: AfterIngestedCtx<'_>) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

pub type SharedImportHook = Arc<dyn ImportHook + Send + Sync>;
