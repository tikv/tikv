// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::storage_api::*;
use std::fmt;

/// Raw bytes of the request payload from the client to the coprocessor.
pub type RawRequest = [u8];
/// The response from the coprocessor encoded as raw bytes that are sent back to the client.
pub type RawResponse = Vec<u8>;

/// Error returned by a plugin.
///
/// [`PluginError`] is currently only used to encapsulate errors that occurred in TiKV and are not
/// handled by the coprocessor plugin itself. If a plugin wants to return a custom error, e.g. an
/// error in the business logic, the plugin should return an appropriately encoded error in
/// [`RawResponse`]; in other words, plugins are responsible for their error handling by themselves.
#[derive(Debug)]
pub enum PluginError {
    StorageError(StorageError),
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PluginError::StorageError(err) => {
                write!(f, "Error while executing a coprocessor plugin: {:?}", err)
            }
        }
    }
}

impl std::error::Error for PluginError {}

impl From<StorageError> for PluginError {
    fn from(err: StorageError) -> Self {
        PluginError::StorageError(err)
    }
}

/// A plugin that allows users to execute arbitrary code on TiKV nodes.
///
/// If you want to implement a custom coprocessor plugin for TiKV, your plugin needs to implement
/// the [`CoprocessorPlugin`] trait.
///
/// Plugins can run setup code in their constructor and teardown code by implementing
/// [`std::ops::Drop`].
pub trait CoprocessorPlugin: Send + Sync {
    /// Returns the name of the plugin.
    /// Requests that are sent to TiKV coprocessor must have a matching `copr_name` field.
    fn name(&self) -> &'static str;

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
        region: &Region,
        request: &RawRequest,
        storage: &dyn RawStorage,
    ) -> Result<RawResponse, PluginError>;
}
