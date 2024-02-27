// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

mod key;
mod list;
mod memory_control;

pub use key::KeyComparator;
pub use list::{IterRef, Node, Skiplist};
pub use memory_control::{AllocationRecorder, MemoryController};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Bound<T> {
    /// An inclusive bound.
    Included(T),
    /// An exclusive bound.
    Excluded(T),
    /// An infinite endpoint. Indicates that there is no bound in this
    /// direction.
    Unbounded,
}

#[derive(Debug)]
pub enum Error {
    MemoryAcquireFailed,
}
