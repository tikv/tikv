// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Implementation of engine_traits for RaftEngine
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! TiKV to persist its data.
//!
//! The module structure here mirrors that in engine_traits where possible.
//!
//! Because there are so many similarly named types across the TiKV codebase,
//! and so much "import renaming", this crate consistently explicitly names type
//! that implement a trait as `RocksTraitname`, to avoid the need for import
//! renaming and make it obvious what type any particular module is working with.
//!
//! Please read the engine_trait crate docs before hacking.

#![cfg_attr(test, feature(test))]
#![feature(generic_associated_types)]

#[macro_use]
extern crate tikv_util;

mod engine;
pub use engine::{RaftEngineConfig, RaftLogBatch, RaftLogEngine, ReadableSize, RecoveryMode};
