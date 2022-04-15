// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod apply;
pub mod compaction;
mod concat_iterator;
pub mod dfs;
pub mod engine;
pub mod engine_trait;
mod error;
pub mod flush;
pub mod meta;
pub mod options;
pub mod prepare;
pub mod read;
pub mod shard;
pub mod split;
pub mod stats;
pub mod table;
pub mod write;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog_global;
extern crate core;

mod metrics;
#[cfg(test)]
mod tests;

pub use apply::*;
pub use compaction::*;
use concat_iterator::ConcatIterator;
pub use engine::*;
pub use error::*;
use flush::*;
pub use meta::*;
pub use options::*;
pub use prepare::*;
pub use read::*;
pub use shard::*;
pub use split::*;
pub use stats::*;
pub use table::table::Iterator;
pub use write::*;

const NUM_CFS: usize = 3;
const CF_LEVELS: [usize; NUM_CFS] = [3, 2, 1];
const CF_MANAGED: [bool; NUM_CFS] = [true, false, true];
