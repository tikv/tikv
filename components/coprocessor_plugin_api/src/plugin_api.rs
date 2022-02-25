// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Range;

use super::storage_api::*;
use crate::PluginResult;

/// Raw bytes of the request payload from the client to the coprocessor.
pub type RawRequest = Vec<u8>;
/// The response from the coprocessor encoded as raw bytes that are sent back to the client.
pub type RawResponse = Vec<u8>;

/// A plugin that allows users to execute arbitrary code on TiKV nodes.
///
/// If you want to implement a custom coprocessor plugin for TiKV, your plugin needs to implement
/// the [`CoprocessorPlugin`] trait.
///
/// Plugins can run setup code in their constructor and teardown code by implementing
/// [`std::ops::Drop`].
pub trait CoprocessorPlugin: Send + Sync {
    /// Handles a request to the coprocessor.
    ///
    /// The data in the `request` parameter is exactly the same data that was passed with the
    /// `RawCoprocessorRequest` in the `data` field. Each plugin is responsible to properly decode
    /// the raw bytes by itself.
    /// The same is true for the return parameter of this function. Upon successful completion, the
    /// function should return a properly encoded result as raw bytes which is then sent back to
    /// the client.
    ///
    /// Most of the time, it's a good idea to use Protobuf for encoding/decoding, but in general you
    /// can also send raw bytes.
    ///
    /// Plugins can read and write data from the underlying [`RawStorage`] via the `storage`
    /// parameter.
    fn on_raw_coprocessor_request(
        &self,
        ranges: Vec<Range<Key>>,
        request: RawRequest,
        storage: &dyn RawStorage,
    ) -> PluginResult<RawResponse>;
}
