// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use super::plugin_registry::PluginRegistry;
use crate::storage::Engine;
use kvproto::coprocessor_v2 as coprv2pb;

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct Endpoint<E: Engine> {
    plugin_registry: Arc<PluginRegistry>,
    _phantom: PhantomData<E>,
}

impl<E: Engine> tikv_util::AssertSend for Endpoint<E> {}

impl<E: Engine> Endpoint<E> {
    pub fn new() -> Self {
        Self {
            plugin_registry: Arc::new(PluginRegistry::new()),
            _phantom: Default::default(),
        }
    }

    /// Handles a request to the coprocessor framework.
    ///
    /// Each request is dispatched to the corresponding coprocessor plugin based on it's `copr_name`
    /// field. A plugin with a matching name must be loaded by TiKV, otherwise an error is returned.
    #[inline]
    pub fn handle_request(
        &self,
        _req: coprv2pb::RawCoprocessorRequest,
    ) -> impl Future<Output = coprv2pb::RawCoprocessorResponse> {
        unimplemented!("Coprocessor V2 is currently not implemented.");

        // Make sure we produce a valid return type
        // (because `!` type doesn't implement `Future`).
        #[allow(unreachable_code)]
        std::future::ready(coprv2pb::RawCoprocessorResponse::default())
    }
}

#[cfg(test)]
mod tests {}
