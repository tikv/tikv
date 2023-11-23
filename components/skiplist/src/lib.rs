// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(slice_pattern)]
#![feature(let_chains)]

mod arena;
pub mod gc;
mod key;
mod list;
pub mod memory_engine;

const MAX_HEIGHT: usize = 20;

pub use key::{ByteWiseComparator, FixedLengthSuffixComparator, KeyComparator};
pub use list::{IterRef, Skiplist, MAX_NODE_SIZE};
