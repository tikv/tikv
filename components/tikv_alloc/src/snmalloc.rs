// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::default::*;

pub type Allocator = snmalloc_rs::SnMalloc;

pub const fn allocator() -> Allocator {
    snmalloc_rs::SnMalloc
}
