// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod table;
pub mod shard;
pub mod engine;
pub mod options;
pub mod iterator;
pub mod write;
pub mod meta;
mod error;

pub use table::table::Iterator;
pub use shard::Shard;
pub use engine::Engine;
pub use options::{Options, CFConfig};
pub use error::Error;
pub use write::*;


const NUM_CFS: usize = 3;
const NUM_LEVELS: usize = 3;
