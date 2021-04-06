// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::default::*;

pub type Allocator = mimallocator::Mimalloc;

pub const fn allocator() -> Allocator {
    mimallocator::Mimalloc
}
