// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::marker::PhantomData;

use crate::storage::Engine;
use kvproto::coprocessor_v2 as coprv2pb;

/// A pool to build and run Coprocessor request handlers.
#[derive(Clone)]
pub struct CoprV2Endpoint<E: Engine> {
    _phantom: PhantomData<E>,
}

impl<E: Engine> tikv_util::AssertSend for CoprV2Endpoint<E> {}

impl<E: Engine> CoprV2Endpoint<E> {
    pub fn new() -> Self {
        Self {
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
        todo!("Coprocessor V2 is currently not implemented.");

        // Make sure we produce a valid return type
        // (because `todo!()` doesn't work with `impl Trait`).
        #[allow(unreachable_code)]
        std::future::ready(coprv2pb::RawCoprocessorResponse::default())
    }
}

#[cfg(test)]
mod tests {}
