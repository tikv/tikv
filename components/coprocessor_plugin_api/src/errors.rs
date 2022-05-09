// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{any::Any, fmt, time::Duration};

use crate::Key;

/// Result returned by operations on [`RawStorage`].
pub type PluginResult<T> = std::result::Result<T, PluginError>;

/// Error returned by operations on [`RawStorage`].
///
/// If a plugin wants to return a custom error, e.g. an error in the business logic, the plugin should
/// return an appropriately encoded error in [`RawResponse`]; in other words, plugins are responsible
/// for their error handling by themselves.
#[derive(Debug)]
pub enum PluginError {
    KeyNotInRegion {
        key: Key,
        region_id: u64,
        start_key: Key,
        end_key: Key,
    },
    Timeout(Duration),
    Canceled,

    /// Errors that can not be handled by a coprocessor plugin but should instead be returned to the
    /// client.
    ///
    /// If such an error appears, plugins can run some cleanup code and return early from the
    /// request. The error will be passed to the client and the client might retry the request.
    Other(String, Box<dyn Any>),
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PluginError::KeyNotInRegion { key, region_id, .. } => {
                write!(f, "Key {:?} not found in region {:?}", key, region_id)
            }
            PluginError::Timeout(d) => write!(f, "timeout after {:?}", d),
            PluginError::Canceled => write!(f, "request canceled"),
            PluginError::Other(s, _) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for PluginError {}
