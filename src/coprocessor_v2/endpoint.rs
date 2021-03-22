// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::sync::Arc;

use super::plugin_registry::PluginRegistry;
use kvproto::coprocessor_v2 as coprv2pb;

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct Endpoint {
    plugin_registry: Arc<PluginRegistry>,
}

impl tikv_util::AssertSend for Endpoint {}

impl Endpoint {
    pub fn new() -> Self {
        Self {
            plugin_registry: Arc::new(PluginRegistry::new()),
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
        todo!("Coprocessor V2 is currently not implemented.");

        // Make sure we produce a valid return type
        // (because `todo!()` doesn't work with `impl Trait`).
        #[allow(unreachable_code)]
        std::future::ready(coprv2pb::RawCoprocessorResponse::default())
    }
}

#[cfg(test)]
mod tests {}
