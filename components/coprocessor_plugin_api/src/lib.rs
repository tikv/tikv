// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate contains some necessary types and traits for implementing a custom coprocessor plugin
//! for TiKV.
//!
//! Most notably, if you want to write a custom plugin, your plugin needs to implement the
//! [`CoprocessorPlugin`] trait. The plugin then needs to be compiled to a `dylib`.
//!
//! > Note: Only `dylib` is supported, and not `cdylib` or `staticlib`, because the latter two are
//! > not able to use TiKV's allocator. See also the documentation in [`std::alloc`].
//!
//! In order to make your plugin callable, you need to declare a constructor with the
//! [`declare_plugin`] macro.
//!
//! A plugin can interact with the underlying storage via the [`RawStorage`] trait.
//!
//! # Example
//!
//! ```no_run
//! use coprocessor_plugin_api::*;
//! use std::ops::Range;
//!
//! #[derive(Default)]
//! struct MyPlugin;
//!
//! impl CoprocessorPlugin for MyPlugin {
//!     fn on_raw_coprocessor_request(
//!         &self,
//!         ranges: Vec<Range<Key>>,
//!         request: RawRequest,
//!         storage: &dyn RawStorage,
//!     ) -> Result<RawResponse, PluginError> {
//!         Ok(vec![])
//!     }
//! }
//!
//! declare_plugin!(MyPlugin::default());
//! ```

#[doc(hidden)]
pub mod allocator;
#[doc(hidden)]
pub mod util;

mod errors;
mod plugin_api;
mod storage_api;

pub use errors::*;
pub use plugin_api::*;
pub use storage_api::*;
