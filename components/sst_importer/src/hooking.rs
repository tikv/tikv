// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{any::Any, marker::PhantomData, path::PathBuf, sync::Arc};

use encryption::DataKeyManager;
use futures_util::{future::BoxFuture, lock::OwnedMutexGuard};
use kvproto::import_sstpb::SstMeta;

use crate::errors::Result;

pub struct LocatedSst {
    pub local_path: PathBuf,
    pub meta: SstMeta,
}

#[derive(Clone, Copy)]
pub struct BeforeProposeIngestCtx<'a> {
    pub sst_meta: &'a [SstMeta],
    /// Locate the SST corresponding to the SST Meta.
    ///
    /// This will also load the `total_kvs` and `total_bytes` and fill them in
    /// the returned [`LocatedSst::meta`].
    pub locate: &'a (dyn Send + Sync + Fn(&SstMeta) -> Result<LocatedSst>),
    /// The key manager binding to the importer.
    pub key_manager: &'a Option<Arc<DataKeyManager>>,
}

impl<'a> BeforeProposeIngestCtx<'a> {
    pub fn sst_to_path(&self, meta: &SstMeta) -> Result<PathBuf> {
        (self.locate)(meta).map(|l| l.local_path)
    }
}

#[derive(Clone, Copy)]
pub struct AfterIngestedCtx<'a> {
    pub sst_meta: &'a [SstMeta],
}

pub trait ImportHook: Any {
    fn before_propose_ingest<'a: 'r, 'b: 'r, 'r>(
        &'a self,
        cx: BeforeProposeIngestCtx<'b>,
    ) -> BoxFuture<'r, Result<()>>;
    fn after_ingested<'a: 'r, 'b: 'r, 'r>(&'a self, cx: AfterIngestedCtx<'b>) -> BoxFuture<'r, ()>;
}

#[derive(Default)]
pub struct NopHooks {
    _non_clone: PhantomData<OwnedMutexGuard<()>>,
}

impl ImportHook for NopHooks {
    fn before_propose_ingest<'a: 'r, 'b: 'r, 'r>(
        &'a self,
        cx: BeforeProposeIngestCtx<'b>,
    ) -> BoxFuture<'r, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn after_ingested<'a: 'c, 'b: 'c, 'c>(&'a self, cx: AfterIngestedCtx<'b>) -> BoxFuture<'c, ()> {
        Box::pin(async {})
    }
}

pub type SharedImportHook = Arc<dyn ImportHook + Send + Sync>;
