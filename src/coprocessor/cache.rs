// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use kvproto::coprocessor::Response;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_kv::SnapshotExt;

use crate::{
    coprocessor::{RequestHandler, *},
    storage::Snapshot,
};

pub struct CachedRequestHandler {
    data_version: Option<u64>,
}

impl CachedRequestHandler {
    pub fn new<S: Snapshot>(snap: S) -> Self {
        Self {
            data_version: snap.ext().get_data_version(),
        }
    }

    pub fn builder<S: Snapshot>() -> RequestHandlerBuilder<S> {
        Box::new(|snap, _req_ctx: &ReqContext| Ok(CachedRequestHandler::new(snap).into_boxed()))
    }
}

#[async_trait]
impl RequestHandler for CachedRequestHandler {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<Response>> {
        let mut resp = Response::default();
        resp.set_is_cache_hit(true);
        if let Some(v) = self.data_version {
            resp.set_cache_last_version(v);
        }
        Ok(resp.into())
    }
}
